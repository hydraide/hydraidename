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
	Get() string
	GetFolderNumber(allFolders uint16) uint16
	IsWildcardPattern() bool
	// ComparePattern ----- > ez alatt lévő funkciók már nincsenek a nyilvános SDK-ban, a többi szinkronban van
	ComparePattern(comparableName Name) bool
	GetSanctuaryID() string
	GetRealmName() string
	GetSwampName() string
	GetFullHashPath(rootPath string, islandID uint64, depth int, maxFoldersPerLevel int) string
}

type name struct {
	Path           string
	SanctuaryID    string
	RealmName      string
	SwampName      string
	HashPath       string
	ServerNumber   uint16
	FolderNumber   uint16
	hashPathMu     sync.Mutex
	folderNumberMu sync.Mutex
}

// New creates a new empty Name instance.
// Use this as the starting point for building hierarchical names
// by chaining Sanctuary(), Realm(), and Swamp().
func New() Name {
	return &name{}
}

// Sanctuary sets the top-level domain of the Name.
// Typically used to group major logical areas (e.g. "users", "products").
func (n *name) Sanctuary(sanctuaryID string) Name {
	return &name{
		SanctuaryID: sanctuaryID,
		Path:        sanctuaryID,
	}
}

// Realm sets the second-level scope under the Sanctuary.
// Often used to further categorize Swamps (e.g. "profiles", "settings").
func (n *name) Realm(realmName string) Name {
	return &name{
		SanctuaryID: n.SanctuaryID,
		RealmName:   realmName,
		Path:        n.Path + "/" + realmName,
	}
}

// Swamp sets the final segment of the Name — the Swamp itself.
// This represents the concrete storage unit where Treasures are kept.
// The full path becomes: sanctuary/realm/swamp.
func (n *name) Swamp(swampName string) Name {
	return &name{
		SanctuaryID: n.SanctuaryID,
		RealmName:   n.RealmName,
		SwampName:   swampName,
		Path:        n.Path + "/" + swampName,
	}
}

// Get returns the full hierarchical path of the Name in the format:
//
//	"sanctuary/realm/swamp"
//
// 🔒 Internal use only: This method is intended for SDK-level logic,
// such as logging, folder path generation, or internal diagnostics.
// SDK users should never need to call this directly.
func (n *name) Get() string {
	return n.Path
}

// GetFolderNumber returns the 1-based index of the server responsible for this Name.
// It uses a fast, consistent xxhash hash over the combined Sanctuary, Realm, and Swamp
// to deterministically assign the Name to one of `allFolders` available slots.
//
// 🔒 Internal use only: This function is used by the SDK to route
// the Name to the correct Hydra client instance in a distributed setup.
// It should not be called directly by application developers.
//
// Example (inside SDK logic):
//
//	client := router.Route(name.GetFolderNumber(1000))
func (n *name) GetFolderNumber(allFolders uint16) uint16 {

	n.folderNumberMu.Lock()
	defer n.folderNumberMu.Unlock()

	if n.FolderNumber != 0 {
		return n.FolderNumber
	}

	hash := xxhash.Sum64([]byte(n.SanctuaryID + n.RealmName + n.SwampName))

	n.FolderNumber = uint16(hash%uint64(allFolders)) + 1

	return n.FolderNumber

}

// IsWildcardPattern returns true if any part of the Name is set to "*".
func (n *name) IsWildcardPattern() bool {
	return n.SanctuaryID == "*" || n.RealmName == "*" || n.SwampName == "*"
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

func (n *name) GetFullHashPath(rootPath string, islandID uint64, depth int, maxFoldersPerLevel int) string {

	n.hashPathMu.Lock()
	defer n.hashPathMu.Unlock()

	if n.HashPath != "" {
		return n.HashPath
	}

	hashedDirectoryPath := generateHashedDirectoryPath(n.Path, depth, maxFoldersPerLevel)
	n.HashPath = filepath.Join(rootPath, fmt.Sprintf("%d", islandID), hashedDirectoryPath)
	return n.HashPath

}

// Load reconstructs a Name from a given path string in the format:
//
//	"sanctuary/realm/swamp"
//
// It parses the path segments and returns a Name instance with all fields set.
//
// 🔒 Internal use only: This function is intended for SDK-level logic,
// such as reconstructing a Name from persisted references, file paths, or routing metadata.
// It should not be called by application developers directly.
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
