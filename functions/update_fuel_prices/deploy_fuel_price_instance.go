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
	log.Print("[FUNC-INFO] Starting function execution")
	functions.HTTP("DeployInstance", DeployInstance)
}

func DeployInstance(w http.ResponseWriter, r *http.Request) {
	log.Print("[FUNC-INFO] Deploying instance")

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

	_, err = createInstance(cs)
	if err != nil {
		log.Fatal(err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("creating instance " + instanceName + "in zone: " + zone))
		os.Exit(1)
	}

	log.Print(w[FUNC-INFO] , "Function execution finish")
}

func initComputeService() (*compute.Service, error) {
	ctx := context.Background()
	return compute.NewService(ctx)
}

func createInstance(computeService *compute.Service) (*compute.Operation, error) {
	fmt.Printf("[FUNC-INFO] Creating Compute Instance with: instanceName = %s, zone = %s, machineType = %s", instanceName, zone, machineType)
	instance := &compute.Instance{
		Name:              instanceName,
		MachineType:       fmt.Sprintf("zones/%s/machineTypes/%s", zone, machineType),
		NetworkInterfaces: getNetworkInterfaces(),
		Scheduling:        getScheduling(),
		Disks:             getAttachedDisks(),
		Metadata:          getMetadata(),
	}
	fmt.Printf("[FUNC-INFO] Calling computeService.Instances.Insert with PROJECT_ID = %s, ZONE = %s, INSTANCE_NAME = %s", projectID, zone, instanceName)
	return computeService.Instances.Insert(projectID, zone, instance).Do()
}

func getNetworkInterfaces() []*compute.NetworkInterface {
	fmt.Printf("[FUNC-INFO] Creating Network Interface obj with PROJECT_ID = %s, REGION = %s", projectID, region)
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
	fmt.Println("[FUNC-INFO] Creating Scheduling obj")
	return &compute.Scheduling{Preemptible: true}
}

func getAttachedDisks() []*compute.AttachedDisk {
	fmt.Println("[FUNC-INFO] Creating Attached Disk obj")
	return []*compute.AttachedDisk{
		{
			Boot:       true,         // The first disk must be a boot disk.
			AutoDelete: true,         // Optional
			Mode:       "READ_WRITE", // Mode should be READ_WRITE or READ_ONLY
			Interface:  "SCSI",       // SCSI or NVME - NVME only for SSDs
			InitializeParams: &compute.AttachedDiskInitializeParams{
				DiskName:    "worker-instance-boot-disk",
				SourceImage: "projects/debian-cloud/global/images/family/debian-12",
			},
		},
	}
}

func getMetadata() *compute.Metadata {
	fmt.Println("[FUNC-INFO] Creating Metadata obj")
	startupMetadata := "#! /bin/bash\n\necho \"I am STARTING some work at $(date)\" | sudo tee -a $HOME/work.txt"
	shutdownMetadata := "#! /bin/bash\n\necho \"I am FINISHING some work at $(date)\" | sudo tee -a $(HOME)/work.txt"

	return &compute.Metadata{
		Items: []*compute.MetadataItems{
			{
				Key:   "startup-script",
				Value: &startupMetadata,
			},
			{
				Key:   "shutdown-script",
				Value: &shutdownMetadata,
			},
		},
	}
}
