package feature

import (
	"math/rand"
	"sync"
	"time"
)

var (
	once   sync.Once
	random *rand.Rand
)

// Init инициализирует генератор случайных чисел
func Init() {
	once.Do(func() {
		random = rand.New(rand.NewSource(time.Now().UnixNano()))
	})
}

// ShouldUseNewService определяет, нужно ли использовать новый сервис
// на основе процента трафика, указанного в featureFlag (0.0 - 1.0)
func ShouldUseNewService(featureFlag int) bool {
	// Seed the random number generator
	rand.Seed(time.Now().UnixNano())

	return rand.Intn(100)+1 <= featureFlag
}
