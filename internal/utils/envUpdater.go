package utils

import (
	"fmt"
	"os"
	"regexp"
	"strings"
)

func UpdateEnvFile(filename, key, value string) error {
	content, err := os.ReadFile(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return CreateNewEnvFile(filename, key, value)
		}
		return err
	}

	text := string(content)

	pattern := regexp.MustCompile(fmt.Sprintf(`(?m)^\s*%s\s*=\s*.*$`, regexp.QuoteMeta(key)))

	if pattern.MatchString(text) {
		replacement := fmt.Sprintf("%s=%s", key, value)
		text = pattern.ReplaceAllString(text, replacement)
	} else {
		text = strings.TrimSpace(text) + fmt.Sprintf("\n%s=%s\n", key, value)
	}

	return os.WriteFile(filename, []byte(text), 0644)
}

func CreateNewEnvFile(filename, key, value string) error {
	content := fmt.Sprintf("%s=%s\n", key, value)
	return os.WriteFile(filename, []byte(content), 0644)
}
