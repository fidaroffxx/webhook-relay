package dto

import (
	"fmt"
	"net/url"
)

type CreateSubscriptionRequest struct {
	Name      string `json:"name" validate:"required, notEmpty"`
	TargetUrl string `json:"targetUrl" validate:"required, notEmpty"`
}

func NewCreateSubscriptionRequest() *CreateSubscriptionRequest {
	return &CreateSubscriptionRequest{}
}

func (r *CreateSubscriptionRequest) Validate() error {
	if r.Name == "" {
		return fmt.Errorf("name is required")
	}

	nameInByte := []byte(r.Name)
	if len(nameInByte) > 255 {
		return fmt.Errorf("name is too long")
	}

	if r.TargetUrl == "" {
		return fmt.Errorf("target_url is required")
	}

	validateUrl, err := isValidUrl(r.TargetUrl)
	if err != nil {
		return err
	}

	if !validateUrl {
		return fmt.Errorf("target_url is invalid")
	}

	return nil
}

func isValidUrl(subUrl string) (bool, error) {
	parsedUrl, err := url.Parse(subUrl)
	if err != nil {
		return false, fmt.Errorf("erros while parsing subUrl %v", err)
	}

	if parsedUrl == nil || parsedUrl.Scheme == "" || parsedUrl.Host == "" {
		return false, nil
	}

	if parsedUrl.Scheme != "http" && parsedUrl.Scheme != "https" {
		return false, nil
	}

	return true, nil
}
