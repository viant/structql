package domain

import "time"

type Record struct {
	ID            int
	Name          string
	Active        bool
	ActivatedAt   *time.Time
	DeactivatedAt *time.Time
	Amount        *float64
}
