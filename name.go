// Package name segít elosztani a szerverek között az adatokat egyenletesen.
// Ehhez meg kell adni, hogy összesen hány szerver között akarjuk az adatokat elosztani, pl 1000 szerver között.
// meg kell adni, hogy egy szerveren belül hány mélységben legyenek a mappák, pl 2 mélységben.
// és meg kell adni, hogy egy mélységben hány folder lehet maximum.
// A name csomag segít a szerverek közötti elosztásban, a megadott paraméterek alapján.
//
// Ha pl van swampunk, ami egy Sancuary/Realm/Swamp névtérből áll, a rendszer a megadott paramétereknek megfelelően
// kiszámolja, hogy az adott swamp melyik szerveren lesz tárolva, és a megfelelő mappastruktúrát is előállítja az adott
// szerveren.
//
// Minden swamp saját mappában lesz tárolva.
//
// Az előállított mappastruktúra a következőképpen néz ki pl:
// - 600/ba22/703a
// - 774/e81a/9e24
//
// A mappák nevei egyediek lesznek minden esetben és egyenletesen kerülnek elosztásra a szerverek között.
//
// Mire való a szervek száma?
// A legtöbb esetben nem 1000 szerverrel indulunk, de ha pl 1000 szervert állítunk be alapból, akkor amikor növelnünk kell
// a szerverek számát, akkor az első 500 szervernyi adatot az A szerveren hagyjuk, és a második 500 szervernyi adatot a
// B szerverre költöztetjük. Ilyen módon mindkét szerveren újabb hely keletkezik és így könnyen lehet növelni a szerverek
// számát.
//
// Adatok elérése a két szerveren.:
// Ha már volt egy szerverünk A szerver, csak beállítunk egy B szervert, majd az 500-1000 szerverig átmozgatjuk oda az adatokat.
// Ezt követően létrehozunk egy újabb kliens példányt a kódunkban, ami a B szerverre mutat.
// Amikor a name csomaggal előállítjuk a swamp nevét, akkor visszakapjuk a szerver számát is a GetServerNumber függvénnyel
// A szerver számának függvényében kell a kliens példányt meghívni. Így ha a swamppunk 500 alatti szerveren van, akkor az A klienssel,
// ha 500 feletti szerveren van, akkor a B klienssel kell kommunikálni. Ennyire egyszerű, és így nincs szükség arra,
// hogy egy külső plusz organizátor szervert használjunk. A kódból kezelhető minden.
package name

import (
	"fmt"
	"github.com/cespare/xxhash/v2"
	"path/filepath"
	"strings"
	"sync"
)

type Name interface {
	Sanctuary(sanctuaryID string) Name
	Realm(realmName string) Name
	Swamp(swampName string) Name
	ComparePattern(comparableName Name) bool
	GetSanctuaryID() string
	GetRealmName() string
	GetSwampName() string
	Get() string
	GetFullHashPath(rootPath string, allServers int, depth int, maxFoldersPerLevel int) string
	GetServerNumber(allServers int) uint16
}

type name struct {
	Path           string
	SanctuaryID    string
	RealmName      string
	SwampName      string
	HashPath       string
	ServerNumber   uint16
	hashPathMu     sync.Mutex
	folderNumberMu sync.Mutex
}

func New() Name {
	return &name{}
}

func (n *name) Sanctuary(sanctuaryID string) Name {
	return &name{
		SanctuaryID: sanctuaryID,
		Path:        sanctuaryID,
	}
}

func (n *name) Realm(realmName string) Name {
	return &name{
		SanctuaryID: n.SanctuaryID,
		RealmName:   realmName,
		Path:        n.Path + "/" + realmName,
	}
}

func (n *name) Swamp(swampName string) Name {
	return &name{
		SanctuaryID: n.SanctuaryID,
		RealmName:   n.RealmName,
		SwampName:   swampName,
		Path:        n.Path + "/" + swampName,
	}
}

// ComparePattern compares the last element of the path with the given SwampName
func (n *name) ComparePattern(comparableName Name) bool {
	if n.SanctuaryID != comparableName.GetSanctuaryID() {
		return false
	}
	if comparableName.GetRealmName() != "*" && n.RealmName != comparableName.GetRealmName() {
		return false
	}
	if comparableName.GetSwampName() != "*" && n.SwampName != comparableName.GetSwampName() {
		return false
	}
	return true
}

func (n *name) GetSanctuaryID() string {
	return n.SanctuaryID
}

func (n *name) GetRealmName() string {
	return n.RealmName
}

func (n *name) GetSwampName() string {
	return n.SwampName
}

func (n *name) Get() string {
	return n.Path
}

func (n *name) GetFullHashPath(rootPath string, allServers int, depth int, maxFoldersPerLevel int) string {

	n.hashPathMu.Lock()
	defer n.hashPathMu.Unlock()

	if n.HashPath != "" {
		return n.HashPath
	}

	serverNumber := n.GetServerNumber(allServers)
	hashedDirectoryPath := generateHashedDirectoryPath(n.Path, depth, maxFoldersPerLevel)
	n.HashPath = filepath.Join(rootPath, fmt.Sprintf("%d", serverNumber), hashedDirectoryPath)
	return n.HashPath

}

func (n *name) GetServerNumber(allServers int) uint16 {

	n.folderNumberMu.Lock()
	defer n.folderNumberMu.Unlock()

	if n.ServerNumber != 0 {
		return n.ServerNumber
	}

	hash := xxhash.Sum64([]byte(n.SanctuaryID + n.RealmName + n.SwampName))

	n.ServerNumber = uint16(hash%uint64(allServers)) + 1

	return n.ServerNumber

}

func Load(path string) Name {

	// feldolgozzuk a path-ot és előállítjuk belőle a Name objektumot
	// a sanctuaryID, realmName és swampName értékeket a path-ból kell kinyerni
	// ehhez szétsplitteljük a path-ot a / karakter mentén
	// a sanctuaryID az első elem
	// a realmName a második elem
	// a swampName az utolsó elem
	splitPath := strings.Split(path, "/")
	sanctuaryID := splitPath[0]
	realmName := splitPath[1]
	swampName := splitPath[2]

	return &name{
		Path:        sanctuaryID + "/" + realmName + "/" + swampName,
		SanctuaryID: sanctuaryID,
		RealmName:   realmName,
		SwampName:   swampName,
	}

}

func generateHashedDirectoryPath(input string, depth int, maxFoldersPerLevel int) string {
	hash := xxhash.Sum64String(input)
	hashHex := fmt.Sprintf("%x", hash)

	charsPerLevel := len(fmt.Sprintf("%x", maxFoldersPerLevel-1))
	if charsPerLevel < 2 {
		charsPerLevel = 2
	}

	parts := make([]string, depth)
	for i := 0; i < depth; i++ {
		start := i * charsPerLevel
		end := start + charsPerLevel
		if end > len(hashHex) {
			end = len(hashHex)
		}
		parts[i] = hashHex[start:end]
	}

	return strings.Join(parts, "/")
}
