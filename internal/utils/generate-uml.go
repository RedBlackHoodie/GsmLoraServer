package utils

import (
	"fmt"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
)

func GeneratePuml(outputFileName string) error {
	file, err := os.Create(outputFileName)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer file.Close()

	fmt.Fprintln(file, "@startuml")
	fmt.Fprintln(file, "title Go Module Interactions")

	err = filepath.Walk(".", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if filepath.Ext(path) != ".go" {
			return nil
		}

		fset := token.NewFileSet()
		node, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			return nil
		}

		packageName := node.Name.Name
		for _, imp := range node.Imports {
			importPath := imp.Path.Value[1 : len(imp.Path.Value)-1]
			fmt.Fprintf(file, "[%s] --> [%s]\n", packageName, importPath)
		}
		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to walk directory: %w", err)
	}

	fmt.Fprintln(file, "@enduml")

	return nil
}
