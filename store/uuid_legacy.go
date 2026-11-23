//go:build !go1.27

package store

import "github.com/google/uuid"

func UUID() (string, error) {
	u, err := uuid.NewV7()
	if err != nil {
		return "", err
	}
	return u.String(), nil
}
