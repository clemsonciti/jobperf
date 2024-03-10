package main

import (
	"fmt"
	"strconv"
)

var (
	// Default configuration values. These can be overridden with -ldflags.

	defaultSupportURL      = "https://github.com/clemsonciti/jobperf/issues"
	defaultDocsURL         = "https://github.com/clemsonciti/jobperf"
	defaultUseOpenOnDemand = "false"
	defaultOpenOnDemandURL = ""
)

type config struct {
	SupportURL string
	DocsURL    string

	UseOpenOnDemand bool
	OpenOnDemandURL string
}

func loadConfig() (*config, error) {
	// TODO: support loading config from file

	parsedDefaultUseOpenOnDemand, err := strconv.ParseBool(defaultUseOpenOnDemand)
	if err != nil {
		return nil, fmt.Errorf("failed to parse defaultUseOpenOnDemand: %w", err)
	}

	return &config{
		SupportURL:      defaultSupportURL,
		DocsURL:         defaultDocsURL,
		UseOpenOnDemand: parsedDefaultUseOpenOnDemand,
		OpenOnDemandURL: defaultOpenOnDemandURL,
	}, nil
}
