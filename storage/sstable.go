package storage

import (
	"fmt"
	"io/ioutil"
	"lsmgo/lib"
	"lsmgo/lib/config"
	"os"
	"strconv"
	"strings"
)

type writeSession struct {
	lastWroteIndex     int
	wroteIndexBytesLen int
}

type indexRecord struct {
	key    string
	offset int64
}

type indexTable struct {
	tableFileNum int64
	indexes      *[]indexRecord
}

func writeToSstable(storage *storage) {
	var fileNum int64 = 0
	files, err := ioutil.ReadDir(config.ApplicationConfig.FILES_LOCATION.SSTABLES_PATH + "/data")
	if err != nil {
		fmt.Println(err)
		return
	}

	for i := len(files) - 1; i >= 0; i-- {
		fileNameParts := strings.Split(files[i].Name(), ".")
		if len(fileNameParts) < 3 || fileNameParts[0] != "data" || fileNameParts[2] != "sst" {
			continue
		}

		_fileNum, err := strconv.ParseInt(fileNameParts[1], 10, 0)
		if err != nil {
			continue
		}
		if _fileNum > fileNum {
			fileNum = _fileNum
		}
	}

	fileNum += 1

	file := lib.OpenFile(config.ApplicationConfig.FILES_LOCATION.SSTABLES_PATH + "/data/data." + strconv.FormatInt(fileNum, 10) + ".sst")
	indexFile := lib.OpenFile(config.ApplicationConfig.FILES_LOCATION.SSTABLES_PATH + "/indexes/indexes." + strconv.FormatInt(fileNum, 10) + ".ssi")
	preOrderSSTableWrite(storage.tree, &writeSession{}, file, indexFile)
	indexFile.Seek(0, 0)
	SSIndex, err = parseIndexFile(SSIndex, indexFile, fileNum)
	if err != nil {
		fmt.Println(err)
	}
	storage.tree = nil
	storage.size = 0
}

func parseIndexFile(ssTable *[]indexTable, indexFile *os.File, fileNum int64) (*[]indexTable, error) {
	indexData := *ssTable
	indexStat, err := indexFile.Stat()
	if err != nil {
		return ssTable, err
	}
	indexBytes := make([]byte, indexStat.Size())
	readedBytesLen, err := indexFile.Read(indexBytes)
	if err != nil {
		return ssTable, err
	}

	indexStringData := string(indexBytes[0:readedBytesLen])

	indexes := strings.Split(indexStringData, ";")
	indexes = indexes[:len(indexes)-1]
	indexSlice := make([]indexRecord, len(indexes), len(indexes))
	indexData = append(indexData, indexTable{
		tableFileNum: fileNum,
		indexes:      &indexSlice,
	})

	for j := 0; j < len(indexes); j++ {
		index := strings.Split(indexes[j], ":")
		offset, err := strconv.ParseInt(index[1], 10, 0)
		if err != nil {
			return ssTable, err
		}

		(*indexData[len(indexData)-1].indexes)[j] = indexRecord{key: index[0], offset: offset}
	}
	return &indexData, nil
}

func readSSIndex() *[]indexTable {
	files, err := ioutil.ReadDir(config.ApplicationConfig.FILES_LOCATION.SSTABLES_PATH + "indexes/")
	if err != nil {
		fmt.Println(err)
		return nil
	}

	var indexData = make([]indexTable, 0, len(files))
	ssTable := &indexData
	for i := 0; i < len(files); i++ {
		fileNameParts := strings.Split(files[i].Name(), ".")

		if len(fileNameParts) < 3 || fileNameParts[2] != "ssi" {
			continue
		}
		fileNum, err := strconv.ParseInt(fileNameParts[1], 10, 0)

		file, err := os.Open(config.ApplicationConfig.FILES_LOCATION.SSTABLES_PATH + "indexes/" + files[i].Name())
		if err != nil {
			fmt.Println(err)
			return nil
		}

		ssTable, err = parseIndexFile(ssTable, file, fileNum)
		if err != nil {
			fmt.Println(err)
		}
	}
	return ssTable
}

func ReadFromSSTable(key string, offset int64, toOffset int64, ssTableFile *os.File) string {
	_, err := ssTableFile.Seek(offset, 0)
	if err != nil {
		fmt.Println(err)
		return ""
	}

	var ssTableByteData []byte

	if toOffset == -1 {
		ssTableByteData = make([]byte, indexBlockSize, indexBlockSize)
	} else {
		ssTableByteData = make([]byte, toOffset-offset, toOffset-offset)
	}

	to, err := ssTableFile.Read(ssTableByteData)

	if err != nil {
		fmt.Println(err)
		return ""
	}

	ssTableStr := string(ssTableByteData[:to])
	ssTableRecords := strings.Split(ssTableStr, ";")
	ssTableRecords = ssTableRecords[:len(ssTableRecords)-1]
	return findKey(key, &ssTableRecords, offset)
}

func ReadFromSSTables(key string) string {
	var sstableReadData string
	for i := len(*SSIndex) - 1; i >= 0; i-- {
		fileNum := strconv.Itoa(int((*SSIndex)[i].tableFileNum))
		dataFile, err := os.Open(config.ApplicationConfig.FILES_LOCATION.SSTABLES_PATH + "data/data." + fileNum + ".sst")
		if err != nil {
			fmt.Println(err)
			continue
		}

		offset, offsetTo := findOffset((*SSIndex)[i].indexes, key)

		if offset == -1 {
			continue
		}

		sstableReadData = ReadFromSSTable(key, offset, offsetTo, dataFile)
		if sstableReadData == "" {
			continue
		}

		return sstableReadData
	}
	return sstableReadData
}

func findOffset(indexes *[]indexRecord, key string) (int64, int64) {
	var lastNearestIndex int = -1
	minLen := 0
	maxLen := len(*indexes)
	if maxLen == 0 || (maxLen == 1 && ((*indexes)[0].key > key)) {
		return -1, -1
	}

	for {
		i := (minLen + maxLen) / 2
		if (*indexes)[i].key == key {
			lastNearestIndex = i
			break
		} else if (*indexes)[i].key < key {
			lastNearestIndex = i

			if i == minLen || i == maxLen {
				break
			}

			minLen = i
		} else {
			if i == minLen || i == maxLen {
				break
			}

			maxLen = i
		}
	}

	if lastNearestIndex == -1 {
		return -1, -1
	}
	if lastNearestIndex == len(*indexes)-1 {
		return (*indexes)[lastNearestIndex].offset, -1
	}

	return (*indexes)[lastNearestIndex].offset, (*indexes)[lastNearestIndex+1].offset
}

func findKey(key string, ssTableRecords *[]string, offset int64) string {
	maxLen := len(*ssTableRecords)
	minLen := 0

	for {
		i := (minLen + maxLen) / 2

		if i == 1 || i%2 != 0 {
			i--
		} else if i%2 != 0 {
			i++
		}

		strSlice := strings.Split((*ssTableRecords)[i], ":")
		if strSlice[1] == key {
			return strings.Split((*ssTableRecords)[i+1], ":")[1]
		} else if strSlice[1] < key {
			if i == minLen || i == maxLen {
				return ""
			}
			minLen = i
		} else if strSlice[1] > key {
			if i == minLen || i == maxLen {
				return ""
			}
			maxLen = i
		}
	}
}

var SSIndex *[]indexTable = readSSIndex()
