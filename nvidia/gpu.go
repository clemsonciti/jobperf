package nvidia

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"os/exec"
)

// This file has the structs needed to help parse the nvidia-smi XML output.

type SMILog struct {
	XMLName xml.Name `xml:"nvidia_smi_log"`
	GPUs    []GPU    `xml:"gpu"`
}

type GPU struct {
	XMLName             xml.Name      `xml:"gpu"`
	ID                  string        `xml:"id,attr"`
	ProductName         string        `xml:"product_name"`
	ProductBrand        string        `xml:"product_brand"`
	ProductArchitecture string        `xml:"product_architecture"`
	MemoryUsage         FBMemoryUsage `xml:"fb_memory_usage"`
	Utilization         Utilization   `xml:"utilization"`
}

type FBMemoryUsage struct {
	XMLName  xml.Name `xml:"fb_memory_usage"`
	Total    string   `xml:"total"`
	Reserved string   `xml:"reserved"`
	Used     string   `xml:"used"`
	Free     string   `xml:"free"`
}

type Utilization struct {
	XMLName     xml.Name `xml:"utilization"`
	GPUUtil     string   `xml:"gpu_util"`
	MemoryUtil  string   `xml:"memort_util"`
	EncoderUtil string   `xml:"encoder_util"`
	DecoderUtil string   `xml:"decoder_util"`
}

func GetSMILog() (*SMILog, error) {
	cmd := exec.Command("nvidia-smi", "-q", "-x")
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		return nil, fmt.Errorf("failed to run nvidia-smi: %w", err)
	}
	var log SMILog
	err = xml.Unmarshal(out.Bytes(), &log)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal nvidia smi output: %w", err)
	}

	return &log, nil
}
