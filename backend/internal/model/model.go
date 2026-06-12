package model

import (
	"errors"
	"strings"
	"time"
)

const (
	SeveritySEV1 = "SEV1"
	SeveritySEV2 = "SEV2"
	SeveritySEV3 = "SEV3"
	SeveritySEV4 = "SEV4"

	StatusOpen     = "open"
	StatusAcked    = "acked"
	StatusResolved = "resolved"

	EventIncidentCreated       = "incident.created"
	EventIncidentStatusChanged = "incident.status_changed"
)

type Incident struct {
	ID          string    `json:"id"`
	TenantID    string    `json:"tenant_id"`
	Title       string    `json:"title"`
	Description string    `json:"description"`
	Severity    string    `json:"severity"`
	Status      string    `json:"status"`
	Service     string    `json:"service"`
	Assignee    string    `json:"assignee"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	Version     int64     `json:"version"`
}

type CreateIncidentRequest struct {
	Title       string `json:"title"`
	Description string `json:"description"`
	Severity    string `json:"severity"`
	Service     string `json:"service"`
	Assignee    string `json:"assignee"`
}

type UpdateStatusRequest struct {
	Status string `json:"status"`
}

type Event struct {
	EventID        string    `json:"event_id"`
	Type           string    `json:"type"`
	TenantID       string    `json:"tenant_id"`
	IncidentID     string    `json:"incident_id"`
	IdempotencyKey string    `json:"idempotency_key,omitempty"`
	Incident       *Incident `json:"incident,omitempty"`
	Status         string    `json:"status,omitempty"`
	OccurredAt     time.Time `json:"occurred_at"`
}

type APIError struct {
	Error string `json:"error"`
}

func NormalizeTenant(tenant string) string {
	tenant = strings.TrimSpace(tenant)
	if tenant == "" {
		return "demo"
	}
	return tenant
}

func NormalizeSeverity(sev string) string {
	return strings.ToUpper(strings.TrimSpace(sev))
}

func NormalizeStatus(status string) string {
	return strings.ToLower(strings.TrimSpace(status))
}

func ValidateCreate(req CreateIncidentRequest) (CreateIncidentRequest, error) {
	req.Title = strings.TrimSpace(req.Title)
	req.Description = strings.TrimSpace(req.Description)
	req.Severity = NormalizeSeverity(req.Severity)
	req.Service = strings.TrimSpace(req.Service)
	req.Assignee = strings.TrimSpace(req.Assignee)

	if req.Title == "" {
		return req, errors.New("title is required")
	}
	if len(req.Title) > 200 {
		return req, errors.New("title must be <= 200 characters")
	}
	if len(req.Description) > 5000 {
		return req, errors.New("description must be <= 5000 characters")
	}
	if !IsSeverity(req.Severity) {
		return req, errors.New("severity must be one of SEV1, SEV2, SEV3, SEV4")
	}
	return req, nil
}

func ValidateStatus(status string) (string, error) {
	status = NormalizeStatus(status)
	if !IsStatus(status) {
		return status, errors.New("status must be one of open, acked, resolved")
	}
	return status, nil
}

func IsSeverity(sev string) bool {
	switch sev {
	case SeveritySEV1, SeveritySEV2, SeveritySEV3, SeveritySEV4:
		return true
	default:
		return false
	}
}

func IsStatus(status string) bool {
	switch status {
	case StatusOpen, StatusAcked, StatusResolved:
		return true
	default:
		return false
	}
}
