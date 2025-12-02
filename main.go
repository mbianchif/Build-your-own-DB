package main

import (
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
)

func SaveData1(path string, data []byte) error {
	fp, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer fp.Close()

	if _, err = fp.Write(data); err != nil {
		return err
	}

	return fp.Sync() // fsync
}

func SaveData2(path string, data []byte) error {
	tmp := fmt.Sprintf("%s.tmp.%d", path, rand.Int())
	fp, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0664)
	if err != nil {
		return err
	}
	defer func() {
		fp.Close()

		if err != nil {
			os.Remove(tmp)
		}
	}()

	if _, err = fp.Write(data); err != nil {
		return err
	}

	if err = fp.Sync(); err != nil { // fsync
		return err
	}

	return os.Rename(tmp, path)
}

func SaveData3(path string, data []byte) error {
	tmp := fmt.Sprintf("%s.tmp.%d", path, rand.Int())
	fp, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0664)
	if err != nil {
		return err
	}

	if _, err = fp.Write(data); err != nil {
		fp.Close()
		return err
	}

	if err = fp.Sync(); err != nil { // fsync
		fp.Close()
		return err
	}

	fp.Close()
	if err = os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return err
	}

	parentDir := filepath.Dir(path)
	fp, err = os.Open(parentDir)
	if err != nil {
		return err
	}
	defer fp.Close()

	return fp.Sync()
}

func main() {}
