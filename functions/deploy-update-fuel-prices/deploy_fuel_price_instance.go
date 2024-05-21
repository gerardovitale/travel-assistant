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
var computeServiceAccountEmail = ""

func init() {
	functions.HTTP("DeployInstance", DeployInstance)
}

func DeployInstance(w http.ResponseWriter, r *http.Request) {
	log.Print("[FUNC-INFO] Starting function execution")

	projectID = os.Getenv("G_CLOUD_PROJECT_ID")
	zone = os.Getenv("G_CLOUD_COMPUTE_ZONE")
	region = os.Getenv("G_CLOUD_REGION")
	instanceName = os.Getenv("G_CLOUD_COMPUTE_INSTANCE_NAME")
	machineType = os.Getenv("G_CLOUD_COMPUTE_MACHINE_TYPE")
	computeServiceAccountEmail = os.Getenv("G_CLOUD_COMPUTE_SERVICE_ACCOUNT")

	cs, err := initComputeService()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.Fatal(err)
	}

	log.Print("[FUNC-INFO] Deploying instance")
	_, err = createInstance(cs)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("creating instance " + instanceName + "in zone: " + zone))
		log.Fatal(err)
	}

	fmt.Fprintln(w, "OK")
}

func initComputeService() (*compute.Service, error) {
	ctx := context.Background()
	return compute.NewService(ctx)
}

func createInstance(computeService *compute.Service) (*compute.Operation, error) {
	log.Printf(
		"[FUNC-INFO] Creating Compute Instance with: instanceName = %s, zone = %s, machineType = %s",
		instanceName, zone, machineType)
	instance := &compute.Instance{
		Name: instanceName,
		ServiceAccounts: []*compute.ServiceAccount{{Email: computeServiceAccountEmail}},
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
	log.Printf(
		"[FUNC-INFO] Calling computeService.Instances.Insert with G_CLOUD_PROJECT_ID = %s, G_CLOUD_COMPUTE_ZONE = %s, G_CLOUD_COMPUTE_INSTANCE_NAME = %s",
		projectID, zone, instanceName)
	return computeService.Instances.Insert(projectID, zone, instance).Do()
}

func getNetworkInterfaces() []*compute.NetworkInterface {
	log.Printf("[FUNC-INFO] Creating Network Interface obj with G_CLOUD_PROJECT_ID = %s, G_CLOUD_REGION = %s", projectID, region)
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
			Boot:       true,
			AutoDelete: true,
			Mode:       "READ_WRITE",
			Interface:  "SCSI",
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
        image: %s
        env:
          - name: PROD
            value: %s
          - name: G_CLOUD_PROJECT_ID
            value: %s
          - name: G_CLOUD_COMPUTE_INSTANCE_NAME
            value: %s
          - name: G_CLOUD_COMPUTE_SECRET_NAME
            value: %s
          - name: DATA_SOURCE_URL
            value: %s
          - name: DATA_DESTINATION_BUCKET
            value: %s
        stdin: false
        tty: false
    restartPolicy: Never
`
	formatedContainerDeclaration := fmt.Sprintf(
		containerDeclaration,
		os.Getenv("G_CLOUD_COMPUTE_DOCKER_IMAGE_TO_DEPLOY"),
		os.Getenv("PROD"),
		projectID,
		instanceName,
		os.Getenv("G_CLOUD_COMPUTE_SECRET_NAME"),
		os.Getenv("DATA_SOURCE_URL"),
		os.Getenv("DATA_DESTINATION_BUCKET"),
	)
	loggingEnable := "true"
	log.Printf(
		"[FUNC-INFO] Adding Metadata gce-container-declaration = %s and google-logging-enable = %s",
		formatedContainerDeclaration, loggingEnable)
	return &compute.Metadata{
		Items: []*compute.MetadataItems{
			{
				Key:   "gce-container-declaration",
				Value: &formatedContainerDeclaration,
			},
			{
				Key:   "google-logging-enabled",
				Value: &loggingEnable,
			},
		},
	}
}
