// The M2020 IDS NCAM terrain pipeline: a stereo pair of FDR images becomes
// per-eye RAS, DSP, XYM and mesh products through four VICAR steps, each run
// as a Kubernetes pod. The model is the pipeline type; the keys, run id and
// bucket of one run arrive from outside it (Airflow's dag_run.conf).
package TerrainNCAM {
    private import ScalarValues::*;

    // What the pipeline reads and writes, named by the M20 filename convention
    // NLM_1835_0829848458_777FDR_N0874924NCAM00230_0A02LLJ01.VIC: the
    // instrument (NLM/NRM: left/right Navcam), sol, SCLK, a venue character
    // ('_' for a flight FDR, 'M' for a product made on the ground) and the
    // three-letter product type, then the extension.
    package Products {
        part def Product {
            doc /* An IDS image product; its basename carries the fields below. */
            attribute instrument : String;
            attribute sol : Integer;
            attribute sclk : String;
            attribute venue : String;
            attribute productType : String;
            attribute extension : String;
        }
        part def FDR :> Product {
            doc /* Full-frame Data Record: the raw stereo input as downlinked, one per eye. */
            attribute :>> venue = "_";
            attribute :>> productType = "FDR";
            attribute :>> extension = "VIC";
        }
        part def Derived :> Product {
            doc /* A product made on the ground from an FDR: the venue character becomes 'M'. */
            attribute :>> venue = "M";
        }
        part def RAS :> Derived {
            doc /* Radiometrically corrected image (marsrad); the basename every later product of the eye derives from. */
            attribute :>> productType = "RAS";
            attribute :>> extension = "VIC";
        }
        part def DSP :> Derived {
            doc /* Stereo disparity map (marsecorr + marscor3) of one eye against the other. */
            attribute :>> productType = "DSP";
            attribute :>> extension = "img";
        }
        part def XYM :> Derived {
            doc /* Masked XYZ point cloud (marsxyz) of one eye. */
            attribute :>> productType = "XYM";
            attribute :>> extension = "xym";
        }
        part def Mesh :> Derived {
            doc /* Terrain mesh (marsmesh): .obj with .mtl, .png, .lbl and .iv siblings, keeping the RAS token in the basename. */
            attribute :>> productType = "RAS";
            attribute :>> extension = "obj";
        }
    }

    // The VICAR steps. Each is one pod: a wrapper script from the worker image
    // and the cpu, memory (GiB) and OpenMP thread count it is run with.
    package Steps {
        private import Products::*;

        action def VicarStep {
            doc /* A VICAR program run as one Kubernetes pod through its wrapper script. */
            attribute wrapper : String;
            attribute cpu : Integer;
            attribute memoryGi : Integer;
            attribute ompThreads : Integer default = 1;
        }
        action def Radiometric :> VicarStep {
            doc /* marsrad: radiometric correction of one eye's FDR into its RAS. */
            attribute :>> wrapper = "rad_wrapper.sh";
            attribute :>> cpu = 1;
            attribute :>> memoryGi = 1;
            in fdr : FDR;
            out ras : RAS;
        }
        action def Correlate :> VicarStep {
            doc /* marsecorr + marscor3: the disparity of this eye's RAS against the other eye's, OpenMP across two threads. */
            attribute :>> wrapper = "correlate_wrapper.sh";
            attribute :>> cpu = 2;
            attribute :>> memoryGi = 4;
            attribute :>> ompThreads = 2;
            in eye : RAS;
            in other : RAS;
            out disparity : DSP;
        }
        action def XYZ :> VicarStep {
            doc /* marsxyz and its filters: the disparity of one eye into its masked XYZ point cloud. */
            attribute :>> wrapper = "xyz_wrapper.sh";
            attribute :>> cpu = 1;
            attribute :>> memoryGi = 3;
            in eye : RAS;
            in other : RAS;
            in disparity : DSP;
            out xym : XYM;
        }
        action def MeshStep :> VicarStep {
            doc /* marsmesh: the point cloud and its RAS texture into the terrain mesh and its siblings. */
            attribute :>> wrapper = "mesh_wrapper.sh";
            attribute :>> cpu = 2;
            attribute :>> memoryGi = 6;
            in xym : XYM;
            in ras : RAS;
            out mesh : Mesh;
        }
    }

    // The pipeline: both eyes are corrected in parallel, every later step of
    // an eye runs once both RAS products exist, and the two eyes proceed
    // independently to their meshes.
    package Pipeline {
        private import Products::*;
        private import Steps::*;

        action def Terrain {
            doc /* NCAM per-eye terrain mesh generation: FDR -> RAS -> DSP -> XYM -> mesh, left and right. */
            attribute image : String = "tig-worker:latest";

            in leftFdr : FDR;
            in rightFdr : FDR;
            out leftMesh : Mesh;
            out rightMesh : Mesh;

            action rad_left : Radiometric;
            action rad_right : Radiometric;
            action correlate_left : Correlate;
            action correlate_right : Correlate;
            action xyz_left : XYZ;
            action xyz_right : XYZ;
            action mesh_left : MeshStep;
            action mesh_right : MeshStep;

            bind leftFdr = rad_left.fdr;
            bind rightFdr = rad_right.fdr;
            flow rad_left.ras to correlate_left.eye;
            flow rad_right.ras to correlate_left.other;
            flow rad_right.ras to correlate_right.eye;
            flow rad_left.ras to correlate_right.other;
            flow rad_left.ras to xyz_left.eye;
            flow rad_right.ras to xyz_left.other;
            flow correlate_left.disparity to xyz_left.disparity;
            flow rad_right.ras to xyz_right.eye;
            flow rad_left.ras to xyz_right.other;
            flow correlate_right.disparity to xyz_right.disparity;
            flow xyz_left.xym to mesh_left.xym;
            flow rad_left.ras to mesh_left.ras;
            flow xyz_right.xym to mesh_right.xym;
            flow rad_right.ras to mesh_right.ras;
            bind mesh_left.mesh = leftMesh;
            bind mesh_right.mesh = rightMesh;

            first start then eyes;
            fork eyes;
            first eyes then rad_left;
            first eyes then rad_right;
            first rad_left then rasReady;
            first rad_right then rasReady;
            join rasReady;
            first rasReady then correlate;
            fork correlate;
            first correlate then correlate_left;
            first correlate then correlate_right;
            first correlate_left then xyz_left;
            first correlate_right then xyz_right;
            first xyz_left then mesh_left;
            first xyz_right then mesh_right;
            first mesh_left then meshed;
            first mesh_right then meshed;
            join meshed;
            first meshed then done;
        }
    }

    // Where the pipeline runs: one worker node sized like the reference
    // minikube performing the pipeline, and the per-eye products of a run it
    // leaves in the object store. Requirements are stated of these.
    package Deployment {
        private import Products::*;
        private import Pipeline::*;

        part def Worker {
            doc /* A Kubernetes node the pods are scheduled on. */
            attribute cores : Integer = 4;
            attribute memoryGi : Integer = 16;
            perform action terrain : Terrain;
        }
        part def EyeProducts {
            doc /* The products one eye of a run leaves in the object store, all named from the RAS basename. */
            part ras : RAS;
            part dsp : DSP;
            part xym : XYM;
            part mesh : Mesh;
        }
        part ncamWorker : Worker;
        part eyeProducts : EyeProducts;
    }

    package Requirements {
        private import Steps::*;
        private import Deployment::*;

        requirement def CpuCoversThreads {
            doc /* A pod's cpu limit shall be at least its OpenMP thread count, or the threads contend for one core. */
            subject pod : VicarStep;
            require constraint { pod.cpu >= pod.ompThreads }
        }
        requirement <'TN-1'> radLeftCpu : CpuCoversThreads { subject pod = ncamWorker.terrain.rad_left; }
        requirement <'TN-2'> radRightCpu : CpuCoversThreads { subject pod = ncamWorker.terrain.rad_right; }
        requirement <'TN-3'> correlateLeftCpu : CpuCoversThreads { subject pod = ncamWorker.terrain.correlate_left; }
        requirement <'TN-4'> correlateRightCpu : CpuCoversThreads { subject pod = ncamWorker.terrain.correlate_right; }
        requirement <'TN-5'> xyzLeftCpu : CpuCoversThreads { subject pod = ncamWorker.terrain.xyz_left; }
        requirement <'TN-6'> xyzRightCpu : CpuCoversThreads { subject pod = ncamWorker.terrain.xyz_right; }
        requirement <'TN-7'> meshLeftCpu : CpuCoversThreads { subject pod = ncamWorker.terrain.mesh_left; }
        requirement <'TN-8'> meshRightCpu : CpuCoversThreads { subject pod = ncamWorker.terrain.mesh_right; }

        requirement def EyesFitTogether {
            doc /* The two eyes run in parallel: at every stage the left and right pods together shall fit the worker's cores and memory. */
            subject w : Worker;
            require constraint { w.terrain.rad_left.cpu + w.terrain.rad_right.cpu <= w.cores }
            require constraint { w.terrain.correlate_left.cpu + w.terrain.correlate_right.cpu <= w.cores }
            require constraint { w.terrain.xyz_left.cpu + w.terrain.xyz_right.cpu <= w.cores }
            require constraint { w.terrain.mesh_left.cpu + w.terrain.mesh_right.cpu <= w.cores }
            require constraint { w.terrain.rad_left.memoryGi + w.terrain.rad_right.memoryGi <= w.memoryGi }
            require constraint { w.terrain.correlate_left.memoryGi + w.terrain.correlate_right.memoryGi <= w.memoryGi }
            require constraint { w.terrain.xyz_left.memoryGi + w.terrain.xyz_right.memoryGi <= w.memoryGi }
            require constraint { w.terrain.mesh_left.memoryGi + w.terrain.mesh_right.memoryGi <= w.memoryGi }
        }
        requirement <'TN-9'> eyesFitTogether : EyesFitTogether { subject w = ncamWorker; }

        requirement def SharedRasBasename {
            doc /* Per-eye products share the RAS basename: every derived product carries the ground venue, and the mesh keeps the RAS product token. */
            subject p : EyeProducts;
            require constraint { p.dsp.venue == "M" and p.xym.venue == "M" and p.mesh.venue == "M" }
            require constraint { p.mesh.productType == "RAS" }
        }
        requirement <'TN-10'> sharedRasBasename : SharedRasBasename { subject p = eyeProducts; }

        requirement <'TN-11'> correlateConsumesBothEyes {
            doc /* A correlate step consumes the RAS of both eyes, its own first; it cannot start before both radiometric steps finish. Structural: the two RAS pins of Correlate and the rasReady join of Terrain. */
        }
    }

    package Verification {
        private import Requirements::*;
        private import Deployment::*;

        verification def CpuCheck {
            subject w : Worker;
            objective {
                verify radLeftCpu;
                verify radRightCpu;
                verify correlateLeftCpu;
                verify correlateRightCpu;
                verify xyzLeftCpu;
                verify xyzRightCpu;
                verify meshLeftCpu;
                verify meshRightCpu;
                require constraint {
                    w.terrain.rad_left.cpu >= w.terrain.rad_left.ompThreads and
                    w.terrain.rad_right.cpu >= w.terrain.rad_right.ompThreads and
                    w.terrain.correlate_left.cpu >= w.terrain.correlate_left.ompThreads and
                    w.terrain.correlate_right.cpu >= w.terrain.correlate_right.ompThreads and
                    w.terrain.xyz_left.cpu >= w.terrain.xyz_left.ompThreads and
                    w.terrain.xyz_right.cpu >= w.terrain.xyz_right.ompThreads and
                    w.terrain.mesh_left.cpu >= w.terrain.mesh_left.ompThreads and
                    w.terrain.mesh_right.cpu >= w.terrain.mesh_right.ompThreads
                }
            }
            VerificationCases::PassIf(
                w.terrain.rad_left.cpu >= w.terrain.rad_left.ompThreads and
                w.terrain.rad_right.cpu >= w.terrain.rad_right.ompThreads and
                w.terrain.correlate_left.cpu >= w.terrain.correlate_left.ompThreads and
                w.terrain.correlate_right.cpu >= w.terrain.correlate_right.ompThreads and
                w.terrain.xyz_left.cpu >= w.terrain.xyz_left.ompThreads and
                w.terrain.xyz_right.cpu >= w.terrain.xyz_right.ompThreads and
                w.terrain.mesh_left.cpu >= w.terrain.mesh_left.ompThreads and
                w.terrain.mesh_right.cpu >= w.terrain.mesh_right.ompThreads
            )
        }
        verification cpuCheck : CpuCheck { subject w = ncamWorker; }

        verification def FitCheck {
            subject w : Worker;
            objective {
                verify eyesFitTogether;
                require constraint {
                    w.terrain.correlate_left.cpu + w.terrain.correlate_right.cpu <= w.cores and
                    w.terrain.mesh_left.memoryGi + w.terrain.mesh_right.memoryGi <= w.memoryGi
                }
            }
            VerificationCases::PassIf(
                w.terrain.correlate_left.cpu + w.terrain.correlate_right.cpu <= w.cores and
                w.terrain.mesh_left.memoryGi + w.terrain.mesh_right.memoryGi <= w.memoryGi
            )
        }
        verification fitCheck : FitCheck { subject w = ncamWorker; }

        verification def BasenameCheck {
            subject p : EyeProducts;
            objective {
                verify sharedRasBasename;
                require constraint { p.mesh.productType == p.ras.productType and p.mesh.venue == p.ras.venue }
            }
            VerificationCases::PassIf(p.mesh.productType == "RAS" and p.mesh.venue == "M" and p.xym.venue == "M")
        }
        verification basenameCheck : BasenameCheck { subject p = eyeProducts; }
    }

    package Report {
        private import DocumentQueries::*;
        private import KerML::Root::Element;
        private import StandardViewDefinitions::*;

        view terrainFlow : ActionFlowView {
            expose Pipeline::Terrain;
        }

        // The steps of the pipeline: the action usages that carry a pod sizing.
        calc def StepTable :> Query {
            Project(
                source = WhereFeature(
                    source = WhereType(source = OwnedElements(source = Pipeline::Terrain), type = "ActionUsage"),
                    'feature' = "cpu",
                    operator = ">=",
                    value = "0"
                ),
                properties = ("name", "type", "wrapper", "cpu", "memoryGi", "ompThreads")
            )
        }
        calc def ProductTable :> Query {
            Project(
                source = WhereType(source = OwnedElements(source = Products), type = "PartDefinition"),
                properties = ("name", "general", "venue", "productType", "extension", "documentation")
            )
        }
        calc def TraceMatrix :> Query {
            Project(
                source = WhereType(source = OwnedElements(source = Requirements), type = "RequirementUsage"),
                properties = ("shortName", "name", "type"),
                columns = (
                    RelatedColumn(name = "verifiedBy", relationshipKind = "verification", direction = "incoming", maxDepth = 1),
                    RelatedColumn(name = "verified", relationshipKind = "verification", direction = "incoming", maxDepth = 1, aggregate = "any")
                )
            )
        }
        calc def RequirementText :> Query {
            Project(
                source = WhereType(source = OwnedElements(source = Requirements), type = ("RequirementDefinition", "RequirementUsage")),
                properties = ("name", "documentation")
            )
        }

        part def TerrainReport :> Document {
            attribute redefines title = "NCAM Terrain Pipeline";

            part intro : Paragraph {
                attribute redefines text = "The M2020 IDS NCAM terrain pipeline turns a stereo pair of FDR images into per-eye RAS, DSP, XYM and mesh products through four VICAR steps, each a Kubernetes pod. The model is the pipeline type; the keys, run id and bucket of a run are supplied when it is triggered.";
            }
            part steps : Section {
                attribute redefines title = "Steps";
                part table : Table {
                    attribute redefines caption = "Pods of the pipeline and their sizing";
                    calc rows : StepTable;
                }
            }
            part products : Section {
                attribute redefines title = "Products";
                part table : Table {
                    attribute redefines caption = "Products and their M20 filename fields";
                    calc rows : ProductTable;
                }
            }
            part structure : Section {
                attribute redefines title = "Control and data flow";
                part diagram : Diagram {
                    attribute redefines caption = "Both eyes are corrected in parallel; every later step of an eye waits for both RAS products";
                    attribute redefines direction = "LR";
                    ref redefines source = terrainFlow;
                }
            }
            part traceability : Section {
                attribute redefines title = "Requirements";
                part text : Table {
                    attribute redefines caption = "What the pipeline is required to do";
                    calc rows : RequirementText;
                }
                part matrix : Table {
                    attribute redefines caption = "Requirements and the verification cases that cover them";
                    calc rows : TraceMatrix;
                }
            }
        }
    }
}
