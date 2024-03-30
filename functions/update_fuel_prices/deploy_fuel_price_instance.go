package updatefuelprices

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"google.golang.org/api/compute/v1"
)

var projectID = ""
var zone = ""
var region = ""
var instanceName = ""
var machineType = ""

func init() {
	functions.HTTP("DeployInstance", DeployInstance)
}

func DeployInstance(w http.ResponseWriter, r *http.Request) {
	log.Print("[FUNC-INFO] Starting function execution")

	projectID = os.Getenv("PROJECT_ID")
	zone = os.Getenv("ZONE")
	region = os.Getenv("REGION")
	instanceName = os.Getenv("INSTANCE_NAME")
	machineType = os.Getenv("MACHINE_TYPE")

	cs, err := initComputeService()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.Fatal(err)
	}

	log.Print("[FUNC-INFO] Deploying instance")
	_, err = createInstance(cs)
	if err != nil {
		log.Fatal(err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("creating instance " + instanceName + "in zone: " + zone))
		os.Exit(1)
	}

	fmt.Fprintln(w, "OK")
}

func initComputeService() (*compute.Service, error) {
	ctx := context.Background()
	return compute.NewService(ctx)
}

func createInstance(computeService *compute.Service) (*compute.Operation, error) {
	log.Printf("[FUNC-INFO] Creating Compute Instance with: instanceName = %s, zone = %s, machineType = %s", instanceName, zone, machineType)
	instance := &compute.Instance{
		Name: instanceName,
		Labels: map[string]string{
			"goog-ec-src":  "vm_add-rest",
			"container-vm": "cos-stable-109-17800-147-38",
		},
		MachineType:       fmt.Sprintf("zones/%s/machineTypes/%s", zone, machineType),
		NetworkInterfaces: getNetworkInterfaces(),
		Scheduling:        getScheduling(),
		Disks:             getAttachedDisks(),
		Metadata:          getMetadata(),
	}
	log.Printf("[FUNC-INFO] Calling computeService.Instances.Insert with PROJECT_ID = %s, ZONE = %s, INSTANCE_NAME = %s", projectID, zone, instanceName)
	return computeService.Instances.Insert(projectID, zone, instance).Do()
}

func getNetworkInterfaces() []*compute.NetworkInterface {
	log.Printf("[FUNC-INFO] Creating Network Interface obj with PROJECT_ID = %s, REGION = %s", projectID, region)
	return []*compute.NetworkInterface{
		{
			Name:       "default",
			Subnetwork: fmt.Sprintf("projects/%s/regions/%s/subnetworks/default", projectID, region),
			AccessConfigs: []*compute.AccessConfig{
				{
					Name:        "External NAT",
					Type:        "ONE_TO_ONE_NAT",
					NetworkTier: "PREMIUM",
				},
			},
		},
	}
}

func getScheduling() *compute.Scheduling {
	log.Print("[FUNC-INFO] Creating Scheduling obj")
	return &compute.Scheduling{Preemptible: true}
}

func getAttachedDisks() []*compute.AttachedDisk {
	log.Print("[FUNC-INFO] Creating Attached Disk obj")
	return []*compute.AttachedDisk{
		{
			Boot:       true,         // The first disk must be a boot disk.
			AutoDelete: true,         // Optional
			Mode:       "READ_WRITE", // Mode should be READ_WRITE or READ_ONLY
			Interface:  "SCSI",       // SCSI or NVME - NVME only for SSDs
			InitializeParams: &compute.AttachedDiskInitializeParams{
				DiskName:    "worker-instance-boot-disk",
				DiskType:    "projects/travel-assistant-417315/zones/us-central1-a/diskTypes/pd-standard",
				SourceImage: "projects/cos-cloud/global/images/cos-stable-109-17800-147-38",
				DiskSizeGb:  10,
			},
		},
	}
}

func getMetadata() *compute.Metadata {
	log.Print("[FUNC-INFO] Creating Metadata obj")
	containerDeclaration := `
	spec:
	  containers:
	  - name: travel-assistant
	    image: registry.hub.docker.com/gerardovitale/travel-assistant-update-fuel-prices:latest
	    stdin: false
	    tty: false
	  restartPolicy: Never
    `
	loggingEnable := "true"
	log.Printf("[FUNC-INFO] Adding Metadata gce-container-declaration = %s and google-logging-enable = %s", containerDeclaration, loggingEnable)
	return &compute.Metadata{
		Items: []*compute.MetadataItems{
			{
				Key:   "gce-container-declaration",
				Value: &containerDeclaration,
			},

			{
				Key:   "google-logging-enabled",
				Value: &loggingEnable,
			},
		},
	}
}
