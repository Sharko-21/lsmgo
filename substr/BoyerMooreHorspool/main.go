package BoyerMooreHorspool

import (
	"strings"
)

func getImage(substr string) *[300]int {
	var arr [300]int
	subLen := len(substr)

	for i := subLen - 2; i >= 0; i-- {
		if arr[substr[i]] == 0 {
			arr[substr[i]] = (subLen - 1) - i
		}
	}
	if arr[substr[subLen-1]] == 0 {
		arr[substr[subLen-1]] = subLen
	}

	/*for i := subLen - 1; i >= 0; i-- {
		fmt.Println(string(substr[i]), ":", arr[substr[i]])
	}*/

	return &arr
}

func searchIndex(str string, substr string, image *[300]int) int {
	i := len(substr) - 1
	substrLen := len(substr)
	j := i
	oldJ := j
	maxJ := len(str)
	//fmt.Println("substrLen:", substrLen)
	//fmt.Println("strLen:", maxJ)
	for j < maxJ {
		//fmt.Println("J:", j)
		if i < 0 {
			return j + 1
		}
		if substr[i] == str[j] {
			i--
			j--
		} else {
			if i != substrLen-1 {
				//fmt.Println("i:", i)
				z := image[substr[substrLen-1]]
				//fmt.Println(z)
				if z == 0 {
					i = substrLen - 1
					j = oldJ + substrLen
				} else {
					j = oldJ + z
					i = substrLen - 1
				}
				oldJ = j
			} else {
				//fmt.Println("bbbbbbbbbb")
				//fmt.Println(j, str[j])
				z := image[str[j]]

				if z == 0 {
					i = substrLen - 1
					j = oldJ + substrLen
				} else {
					j = oldJ + z
					i = substrLen - 1
				}
				oldJ = j
			}
		}
	}
	return -1
}

func Index(str string, substr string) int {
	substrLen := len(substr)

	switch {
	case substrLen == 0:
		return 0
	case substrLen == 1:
		return strings.IndexByte(str, substr[0])
	case substrLen == len(str):
		if substr == str {
			return 0
		}
		return -1
	case substrLen > len(str):
		return -1
	case true /*here must be max str length*/ :
		/*subrune := []rune(substr)
		strrune := []rune(str)*/
		image := getImage(substr)
		return searchIndex(str, substr, image)
	}
	return 0
}

func Contains(str string, substr string) bool {
	return Index(str, substr) >= 0
}
