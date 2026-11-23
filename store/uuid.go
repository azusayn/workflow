//go:build go1.27

package store

import "uuid"

func UUID() (string, error) {
	return uuid.NewV7().String(), nil
}
