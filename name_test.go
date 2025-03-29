package name

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"path/filepath"
	"testing"
)

const (
	testDataCount = 100000
	allServers    = 100
)

func randomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	result := make([]byte, length)
	for i := range result {
		num, _ := rand.Int(rand.Reader, big.NewInt(int64(len(charset))))
		result[i] = charset[num.Int64()]
	}
	return string(result)
}

func TestGetServerNumberDistribution(t *testing.T) {
	serverCounts := make(map[uint16]int)

	for i := 0; i < testDataCount; i++ {

		// Generáljunk véletlenszerű Sanctuary, Realm és Swamp neveket
		sanctuary := randomString(8)
		realm := randomString(8)
		swamp := randomString(8)

		// Hozzunk létre egy Name objektumot
		n := New().Sanctuary(sanctuary).Realm(realm).Swamp(swamp)
		// Kapjuk meg a szerver számát
		server := n.GetServerNumber(allServers)

		// Növeljük a számlálót a megfelelő szerverhez
		serverCounts[server]++
	}

	// Ellenőrizzük az eloszlást
	for server, count := range serverCounts {
		t.Logf("Szerver %d: %d adat", server, count)
	}

	// Kiszámoljuk az egyenletességet (átlagos elemszám szerverenként)
	expectedPerServer := testDataCount / allServers
	threshold := expectedPerServer / 10 // 10% eltérés megengedett
	for server, count := range serverCounts {
		if count < expectedPerServer-threshold || count > expectedPerServer+threshold {
			t.Errorf("Szerver %d túl sok vagy túl kevés adatot kapott: %d", server, count)
		}
	}
}

func TestGetFullHashPath(t *testing.T) {

	// Paraméterek a teszthez
	rootPath := "/hydra/data"
	allServers := 1000
	depth := 2
	maxFoldersPerLevel := 10000

	// Első Name példány
	name1 := New().
		Sanctuary("Sanctuary1").
		Realm("RealmA").
		Swamp("SwampX")

	// Második Name példány ugyanazokkal az értékekkel
	name2 := New().
		Sanctuary("Sanctuary1").
		Realm("RealmA").
		Swamp("SwampX")

	// Harmadik Name példány különböző értékekkel
	name3 := New().
		Sanctuary("Sanctuary2").
		Realm("RealmB").
		Swamp("SwampY")

	// Teszt: Ugyanaz a hash path generálódik-e az azonos adatokhoz
	hashPath1 := name1.GetFullHashPath(rootPath, allServers, depth, maxFoldersPerLevel)
	hashPath2 := name2.GetFullHashPath(rootPath, allServers, depth, maxFoldersPerLevel)

	fmt.Println(hashPath1)

	if hashPath1 != hashPath2 {
		t.Errorf("Hash path mismatch for identical names: %s != %s", hashPath1, hashPath2)
	}

	// Teszt: Különböző adatok eltérő hash path-ot generálnak-e
	hashPath3 := name3.GetFullHashPath(rootPath, allServers, depth, maxFoldersPerLevel)
	if hashPath1 == hashPath3 {
		t.Errorf("Hash path collision: %s == %s", hashPath1, hashPath3)
	}

	// Kimenet a könnyebb ellenőrzéshez
	fmt.Println("Hash path 1:", hashPath1)
	fmt.Println("Hash path 2:", hashPath2)
	fmt.Println("Hash path 3:", hashPath3)
}

// goos: linux
// goarch: amd64
// pkg: github.com/trendizz/hydra-spine/hydra/name
// cpu: AMD Ryzen 9 5950X 16-Core Processor
// BenchmarkName_Compare
// BenchmarkName_Compare-32    	64480774	        18.77 ns/op
// NEW BenchmarkName_Compare-32    	11431681	       100.4 ns/op
// Newer BenchmarkName_Compare-32    	150973188	         7.443 ns/op
func BenchmarkName_Compare(b *testing.B) {

	swampName := New().Sanctuary("users").Realm("petergebri").Swamp("info")
	pattern := New().Sanctuary("users").Realm("*").Swamp("info")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		swampName.ComparePattern(pattern)
	}

}

// goos: linux
// goarch: amd64
// pkg: github.com/trendizz/hydra-spine/hydra/name
// cpu: AMD Ryzen 9 5950X 16-Core Processor
// BenchmarkName_LoadFromCanonicalForm
// NEW BenchmarkName_Load-32    	 1630828	       748.7 ns/op
// Newer BenchmarkName_Load-32    	 3155320	       373.6 ns/op
func BenchmarkName_Load(b *testing.B) {

	canonicalForm := filepath.Join("users", "petergebri", "info")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = Load(canonicalForm)
	}

}

// goos: linux
// goarch: amd64
// pkg: github.com/trendizz/hydra-spine/hydra/name
// cpu: AMD Ryzen 9 5950X 16-Core Processor
// BenchmarkName_GetCanonicalForm
// NEW BenchmarkName_Get-32    	69597764	        16.46 ns/op
// Newer BenchmarkName_Get-32    	617518238	         1.919 ns/op
func BenchmarkName_Get(b *testing.B) {

	nameObj := New().Sanctuary("users").Realm("petergebri").Swamp("info")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nameObj.Get()
	}

}

// goos: linux
// goarch: amd64
// pkg: github.com/trendizz/hydra-spine/hydra/name
// cpu: AMD Ryzen 9 5950X 16-Core Processor
// BenchmarkName_Add
// BenchmarkName_Add-32    	 2036251	       592.8 ns/op
// Newer BenchmarkName_Add-32    	19829251	        61.39 ns/op
// BenchmarkName_Add-32    	26797796	        39.08 ns/op
func BenchmarkName_Add(b *testing.B) {

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		New().Sanctuary("users").Realm("petergebri").Swamp("info")
	}

}

// goos: linux
// goarch: amd64
// pkg: github.com/trendizz/hydra/hydra/name
// cpu: AMD Ryzen Threadripper 2950X 16-Core Processor
// BenchmarkGetServerNumber
// BenchmarkGetServerNumber-32    	 5146892	       227.8 ns/op
// Newer BenchmarkGetServerNumber-32    	74614863	        16.16 ns/op
// PASS
func BenchmarkGetServerNumber(b *testing.B) {
	// Előkészítünk egy minta nevet
	sanctuary := "BenchmarkSanctuary"
	realm := "BenchmarkRealm"
	swamp := "BenchmarkSwamp"

	// Készítsünk egy `Name` objektumot
	n := New().Sanctuary(sanctuary).Realm(realm).Swamp(swamp)

	b.ResetTimer() // Elindítjuk az időmérést

	for i := 0; i < b.N; i++ {
		_ = n.GetServerNumber(allServers) // Meghívjuk a funkciót
	}
}

// /home/bearbite/.cache/JetBrains/GoLand2024.2/tmp/GoLand/___BenchmarkName_GetFullHashPath_in_github_com_trendizz_hydra_hydra_name.test -test.v -test.paniconexit0 -test.bench ^\QBenchmarkName_GetFullHashPath\E$ -test.run ^$
// goos: linux
// goarch: amd64
// pkg: github.com/trendizz/hydra/hydra/name
// cpu: AMD Ryzen Threadripper 2950X 16-Core Processor
// BenchmarkName_GetFullHashPath
// BenchmarkName_GetFullHashPath-32    	64911246	        15.85 ns/op
func BenchmarkName_GetFullHashPath(b *testing.B) {

	// Előkészítünk egy minta nevet
	sanctuary := "BenchmarkSanctuary"
	realm := "BenchmarkRealm"
	swamp := "BenchmarkSwamp"

	// Készítsünk egy `Name` objektumot
	n := New().Sanctuary(sanctuary).Realm(realm).Swamp(swamp)

	// Paraméterek a teszthez
	rootPath := "/hydra/data"
	allServersForBenchmark := 10
	depth := 3
	maxFoldersPerLevel := 5000

	b.ResetTimer() // Elindítjuk az időmérést

	for i := 0; i < b.N; i++ {
		n.GetFullHashPath(rootPath, allServersForBenchmark, depth, maxFoldersPerLevel) // Meghívjuk a funkciót
	}

}
