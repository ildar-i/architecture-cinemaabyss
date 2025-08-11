package config

// Config содержит основную конфигурацию приложения
type Config struct {
	Server struct {
		Address          string `yaml:"address"`
		GradualMigration bool   `yaml:"gradualMigration"`
	} `yaml:"server"`

	Monolith struct {
		BaseURL string `yaml:"baseURL"`
		Timeout int    `yaml:"timeoutSeconds"`
	} `yaml:"monolith"`

	CinemaMetadata struct {
		BaseURL string `yaml:"baseURL"`
		Timeout int    `yaml:"timeoutSeconds"`
	} `yaml:"cinemaMetadata"`

	Features struct {
		MoviesFeatureFlag float64 `yaml:"moviesFeatureFlag"` // 0.0-1.0 процент трафика на новый сервис
	} `yaml:"features"`
}

// Load загружает конфигурацию из файла
//func Load(path string) (*Config, error) {
//	data, err := ioutil.ReadFile(path)
//	if err != nil {
//		return nil, err
//	}
//
//	var cfg Config
//	if err := yaml.Unmarshal(data, &cfg); err != nil {
//		return nil, err
//	}
//
//	return &cfg, nil
//}
