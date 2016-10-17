package zfs

import (
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/theairkit/runcmd"
)

var (
	testPath   = "tank/test"
	sendPath   = testPath
	otherPool  = "zssd/test"
	sudoPath   = "tank/sudo"
	unicorn    = testPath + "/unicorn"
	badDataset = testPath + "/bad/"
	user       = os.Getenv("TEST_USER")
	pass       = os.Getenv("TEST_PASSWORD")
)

func init() {
	SetStdSudo(true)
}

func TestGetPool(t *testing.T) {
	pool := NewFs("tank/some/thing").GetPool()
	if pool != "tank" {
		t.Error("GetPool: Wrong pool")
	}
}

func TestGetLastPath(t *testing.T) {
	pool := NewFs("tank/some/thing").GetLastPath()
	if pool != "thing" {
		t.Error("[LastPath] Wrong last name")
	}
}

func TestExists(t *testing.T) {
	ok, err := NewFs(testPath).Exists()
	if err != nil {
		t.Error("[Exists]", err)
	}

	if !ok {
		t.Errorf("[Exists] %s exists, but returned false", testPath)
	}

	ok, err = NewFs(unicorn).Exists()
	if err != nil {
		t.Error(err)
	}

	if ok {
		t.Error("[Exists] unicorns doesn't exists, but returned true")
	}

	ok, err = NewFs(badDataset).Exists()
	if ok {
		t.Error("[Exists] returned true on bad dataset")
	}
	if !InvalidDataset.MatchString(err.Error()) {
		t.Error("[Exists] wrong error checking invalid dataset:", err)
	}
}

func TestCreateFs(t *testing.T) {
	fs, err := CreateFs(testPath + "/fs1")
	if err != nil {
		t.Fatal("[CreateFs] error creating fs:", err)
	}
	defer fs.Destroy(RF_Hard)

	if ok, _ := fs.Exists(); !ok {
		t.Fatal("[CreateFs] fs not created!")
	}

	_, err = CreateFs(testPath + "/fs1")
	if err == nil {
		t.Error("[CreateFs] created allready existed fs")
	}
	if !AllreadyExists.MatchString(err.Error()) {
		t.Error("[CreateFs] wrong error creating dup fs:", err)
	}

	_, err = CreateFs(badDataset)
	if err == nil {
		t.Error("[CreateFs] created fs with bad name")
	}
	if !InvalidDataset.MatchString(err.Error()) {
		t.Error("[CreateFs] wrong error while creating fs with bad name:", err)
	}
}

func TestGetProperty(t *testing.T) {
	fs, err := CreateFs(testPath + "/fs1")
	if err != nil {
		t.Fatal("[GetProperty] error creating fs:", err)
	}
	defer fs.Destroy(RF_Hard)

	ty, err := fs.GetProperty("type")
	if err != nil {
		t.Fatal("[GetProperty] error getting property:", err)
	}

	if ty != "filesystem" {
		t.Error(
			"[GetProperty] returned wrong value for 'type': %s,"+
				" want filesystem", t,
		)
	}

	_, err = fs.GetProperty("notexist")
	if err == nil {
		t.Error("[GetProperty] got not existent property")
	}
	if !BadPropGet.MatchString(err.Error()) {
		t.Error("[GetProperty] wrong error getting bad property:", err)
	}
}

func TestSetProperty(t *testing.T) {
	fs, err := CreateFs(testPath + "/fs1")
	if err != nil {
		t.Fatal("[SetProperty] error creating fs:", err)
	}
	defer fs.Destroy(RF_Hard)

	err = fs.SetProperty("quota", "1000000")
	if err != nil {
		t.Error("[SetProperty] error setting property:", err)
	}

	err = fs.SetProperty("oki", "doki")
	if err == nil {
		t.Error("[SetProperty] set bad property")
	}
	if !BadPropSet.MatchString(err.Error()) {
		t.Error("[SetProperty] wrong error setting bad property:", err)
	}
}

func TestSnapshot(t *testing.T) {
	fs, err := CreateFs(testPath + "/fs1")
	if err != nil {
		t.Fatal("[Snapshot] error creating fs:", err)
	}
	defer fs.Destroy(RF_Hard)

	s, err := fs.Snapshot("s1")
	if err != nil {
		t.Fatal("[Snapshot] error creating snapshot:", err)
	}

	spath := testPath + "/fs1@s1"
	if s.Path != spath {
		t.Error("[Snapshot] wrong snapshot path: %s, wanted: %s", s.Path, spath)
	}
	if s.Name != "s1" {
		t.Error("[Snapshot] wrong snapshot name: %s, wanted: %s", s.Name, "s1")
	}
	if s.Fs.Path != testPath+"/fs1" {
		t.Error("[Snapshot] wrong snapshot fs path: %s, wanted: %s",
			s.Fs.Path, testPath+"/fs1")
	}

	if ok, _ := s.Exists(); !ok {
		t.Error("[Snapshot] snapshot not created")
	}

	if ok, _ := NewSnapshot(testPath + "/fs1@s1").Exists(); !ok {
		t.Error("[Snapshot] NewSnapshot not works...")
	}

	_, err = NewFs(unicorn).Snapshot("s2")
	if err == nil {
		t.Error("[Snapshot] created snapshot on not existent fs")
	}
	if !NotExist.MatchString(err.Error()) {
		t.Error("[Snapshot] wrong error creating snap on unicorn:", err)
	}

	_, err = NewFs(badDataset).Snapshot("test")
	if err == nil {
		t.Error("[Snapshot] created snapshot for bad fs")
	}

	if !InvalidDataset.MatchString(err.Error()) {
		t.Error("[Snapshot] wrong error while creating snapshot for bad fs:", err)
	}
}

func TestClone(t *testing.T) {
	fs, err := CreateFs(testPath + "/fs1")
	if err != nil {
		t.Fatal("[Clone] error creating fs:", err)
	}
	defer fs.Destroy(RF_Hard)

	sn, err := fs.Snapshot("s1")
	if err != nil {
		t.Fatal("[Clone] error creating snapshot:", err)
	}

	cl, err := sn.Clone(testPath + "/fs2")
	cl.Destroy(RF_No)
	if err != nil {
		t.Fatal("[Clone] error creating clone:", err)
	}

	cl, err = sn.Clone(testPath + "@qa")
	if err == nil {
		cl.Destroy(RF_Hard)
		t.Error("[Clone] created clone with bad name")
	}

	cl, err = sn.Clone(otherPool + "/fs2")
	if err == nil {
		cl.Destroy(RF_Hard)
		t.Fatal("[Clone] created clone on other pool")
	}
	if err != PoolError {
		t.Errorf("[Clone] wrong error: %s, want %s", err, PoolError)
	}
}

func TestDestroyFs(t *testing.T) {
	fs, err := CreateFs(testPath + "/fs5")
	if err != nil {
		t.Fatal("[DestroyFs] error creating fs:", err)
	}
	defer fs.Destroy(RF_Hard)

	err = fs.Destroy(RF_No)
	if err != nil {
		t.Error("[DestroyFs]", err)
	}

	ok, _ := fs.Exists()
	if ok {
		t.Error("[DestroyFs] fs not deleted")
	}

	// Destroy invalid Dataset
	err = NewFs(badDataset).Destroy(RF_No)
	if !InvalidDataset.MatchString(err.Error()) {
		t.Error("[DestroyFs] wrong error deleting invalid dataset:", err)
	}
}

func TestDestroyRecursiveSoft(t *testing.T) {
	fs, err := CreateFs(testPath + "/fs1")
	if err != nil {
		t.Fatal("[DestroyRecursiveSoft] error creating fs:", err)
	}
	defer fs.Destroy(RF_Hard)

	snap, _ := fs.Snapshot("s1")
	if err != nil {
		t.Fatal("[DestroyRecursiveSoft] error creating snapshot:", err)
	}

	err = fs.Destroy(RF_No)
	if err == nil {
		t.Error(
			"[DestroyRecursiveSoft] destroyed fs with snapshot " +
				"without recursive flag",
		)
	}

	err = fs.Destroy(RF_Soft)
	if err != nil {
		t.Error(err)
	}

	ok, _ := fs.Exists()
	if ok {
		t.Error("[DestroyRecursiveSoft] fs not deleted")
	}

	ok, _ = snap.Exists()
	if ok {
		t.Error("[DestroyRecursiveSoft] snapshot not deleted")
	}
}

func TestDestroyRecursiveHard(t *testing.T) {
	fs, err := CreateFs(testPath + "/fs1")
	if err != nil {
		t.Fatal("[DestroyRecursiveHard] error creating fs:", err)
	}
	defer fs.Destroy(RF_Hard)

	snap, err := fs.Snapshot("s1")
	if err != nil {
		t.Fatal("[DestroyRecursiveHard] error creating snapshot:", err)
	}

	clone, err := snap.Clone(testPath + "/clonedfs")
	if err != nil {
		t.Fatal("[DestroyRecursiveHard] error creating clone:", err)
	}

	t.Log("[DestroyRecursiveHard] destroying not recusively")
	err = fs.Destroy(RF_No)
	if err == nil {
		t.Error(
			"[DestroyRecursiveHard] Destroyed fs with snapshot and clone " +
				"without recursive flag",
		)
	}
	ok, _ := fs.Exists()
	if !ok {
		t.Error("[DestroyRecursiveHard] fs is deleted, but should not")
	}

	ok, _ = snap.Exists()
	if !ok {
		t.Error("[DestroyRecursiveHard] snapshot is deleted, but should not")
	}

	ok, _ = clone.Exists()
	if !ok {
		t.Error("[DestroyRecursiveHard] clone is deleted, but should not")
	}
	if t.Failed() {
		t.FailNow()
	}
	t.Log("[DestroyRecursiveHard] Succed")

	t.Log("[DestroyRecursiveHard] destroying recusively soft")
	err = fs.Destroy(RF_Soft)
	if err == nil {
		t.Fatal(
			"[DestroyRecursiveHard] Destroyed fs with clone with " +
				"soft recursive flag",
		)
	}

	ok, _ = fs.Exists()
	if !ok {
		t.Error("[DestroyRecursiveHard] fs is deleted, but should not")
	}

	ok, _ = snap.Exists()
	if !ok {
		t.Error("[DestroyRecursiveHard] snapshot is deleted, but should not")
	}

	ok, _ = clone.Exists()
	if !ok {
		t.Error("[DestroyRecursiveHard] clone is deleted, but should not")
	}
	if t.Failed() {
		t.FailNow()
	}
	t.Log("[DestroyRecursiveHard] Succed")

	t.Log("[DestroyRecursiveHard] destroying recusively hard")
	err = fs.Destroy(RF_Hard)
	if err != nil {
		t.Error(
			"[DestroyRecursiveHard] error deleting with RF_Hard flag:", err,
		)
	}

	ok, _ = fs.Exists()
	if ok {
		t.Error("[DestroyRecursiveHard] fs not deleted")
	}

	ok, _ = snap.Exists()
	if ok {
		t.Error("[DestroyRecursiveHard] snapshot not deleted")
	}

	ok, _ = clone.Exists()
	if ok {
		t.Error("[DestroyRecursiveHard] clone not deleted")
	}

}

func TestListFs(t *testing.T) {
	want := []string{"", "/fs1", "/fs2", "/fs2/fs3"}

	for _, f := range want[1:] {
		fs, err := CreateFs(testPath + f)
		if err != nil {
			t.Fatal("ListFs error creating fs '%s': %s", testPath+f, err)
		}
		defer fs.Destroy(RF_Hard)
	}

	fs, err := ListFs(testPath)
	if err != nil {
		t.Fatal("[ListFs] error listing fs:", err)
	}

	if len(fs) != len(want) {
		t.Fatal("[ListFs] fs size differs from wanted")
	}
	for i, fs := range fs {
		if fs.Path != testPath+want[i] {
			t.Error("ListFs: fs %s differs from wanted (%s)", fs.Path, want[i])
		}
	}

	fs, err = ListFs(testPath + "/magic/forest")
	if err != nil {
		t.Error("[ListFs]", err)
	}

	if len(fs) > 0 {
		t.Error("[ListFs] found something in magic forest, but it doesn't exists!")
	}

	fs, err = ListFs(badDataset)
	if len(fs) != 0 {
		t.Error("[ListFs] returned not empty fs list on bad dataset:", fs)
	}
	if !InvalidDataset.MatchString(err.Error()) {
		t.Error("[ListFs] wrong error checking invalid dataset:", err)
	}
}

func TestListSnapshots(t *testing.T) {
	fs, err := CreateFs(testPath + "/fs1")
	if err != nil {
		t.Fatal("[ListSnapshots] error creating fs:", err)
	}
	defer fs.Destroy(RF_Hard)
	fs.Snapshot("s1")
	fs.Snapshot("s2")

	want := []string{"/fs1@s1", "/fs1@s2"}
	snaps, err := fs.ListSnapshots()
	if err != nil {
		t.Error("[ListSnapshots]", err)
	}

	for i, snap := range snaps {
		if snap.Path != testPath+want[i] {
			t.Errorf("ListSnapshots: fs %s differs from wanted (%s)",
				snap.Path, want[i])
		}
	}

	_, err = NewFs(badDataset).ListSnapshots()
	if err == nil {
		t.Error("[ListSnapshots] listed bad fs")
	}

	if !InvalidDataset.MatchString(err.Error()) {
		t.Error("[ListSnapshots] wrong error while listing snapshots for bad fs:", err)
	}
}

func TestSudo(t *testing.T) {
	fs, err := CreateFs(sudoPath + "/fs1")
	if err == nil {
		t.Fatal("[Sudo] created without sudo")
	}
	fs.Destroy(RF_Hard)

	if !NotMounted.MatchString(err.Error()) {
		t.Error("[Sudo] wrong error:", err)
	}

	SetStdSudo(true)

	fs, err = CreateFs(sudoPath + "/fs1")
	if err != nil {
		t.Fatal("[Sudo] error creating fs:", err)
	}

	err = fs.Destroy(RF_No)
	if err != nil {
		t.Error("[Sudo] error destroying fs with sudo:", err)
	}

	fs2, err := CreateFs(sudoPath + "/fs2")
	defer fs2.Destroy(RF_Hard)
	if err != nil {
		t.Fatal("[Sudo] error creating fs with sudo:", err)
	}

	SetStdSudo(false)

	err = fs2.Destroy(RF_No)
	if err == nil {
		t.Error("[Sudo] sudo doesn't switch off")
	}

	SetStdSudo(true)
	NewFs(sudoPath + "/fs2").Destroy(RF_No)
}

func TestListClones(t *testing.T) {
	fs, err := CreateFs(testPath + "/fs1")
	if err != nil {
		t.Fatal("[ListClones] error creating fs:", err)
	}
	defer fs.Destroy(RF_Hard)

	sn, err := fs.Snapshot("s1")
	if err != nil {
		t.Fatal("[ListClones] error creating snapshot:", err)
	}

	lclones, err := sn.ListClones()
	if err != nil {
		t.Error("[ListClones]", err)
	}
	if len(lclones) > 0 {
		t.Error("[ListClones] found something wrong")
	}

	clnNames := []string{"cln1", "cln2", "cln3"}

	for _, cln := range clnNames {
		c, err := sn.Clone(testPath + "/" + cln)
		if err != nil {
			t.Fatalf("[ListClones] error creating clone '%s': %s", cln, err)
		}
		defer c.Destroy(RF_Hard)
	}

	lclones, err = sn.ListClones()
	if err != nil {
		t.Error(err)
	}

	if len(lclones) != len(clnNames) {
		fs.Destroy(RF_Hard)
		t.Fatalf("[ListClones] wrong number of clones recived: %d want %d",
			len(lclones), len(clnNames))
	}

	for i, cln := range lclones {
		clonePath := path.Join(testPath, clnNames[i])
		if cln.Path != clonePath {
			t.Error(
				"[ListClones] clone not match: %s want %s",
				cln.Path, clonePath,
			)
		}
	}
}

func TestPromote(t *testing.T) {
	origFs, err := CreateFs(testPath + "/fs6")
	if err != nil {
		t.Fatal("[Promote] error creating original fs:", err)
	}
	defer origFs.Destroy(RF_Hard)

	snap, err := origFs.Snapshot("s1")
	if err != nil {
		t.Fatal("[Promote] error creating snapshot:", err)
	}

	clone, err := snap.Clone(testPath + "/fs7")
	if err != nil {
		t.Fatal("[Promote] error creating clone:", err)
	}
	defer clone.Destroy(RF_Hard)

	newSnap := clone.Path + "@s1"

	err = clone.Promote()
	if err != nil {
		t.Fatal("[Promote] errors while promoting:", err)
	}

	origin, _ := origFs.GetProperty("origin")
	if origin != newSnap {
		t.Errorf(
			"[Promote] original fs have wrong origin %s, want %s",
			origin, newSnap,
		)
	}

	err = clone.Promote()
	if err == nil {
		t.Error("[Promote] promoted not clone fs")
	}
	if !PromoteNotClone.MatchString(err.Error()) {
		t.Error("[Promote] wrong error promoting not clone fs:", err)
	}

	err = NewFs(unicorn).Promote()

	if err == nil {
		t.Error("[Promote] promoted not existed fs")
	}
	if !NotExist.MatchString(err.Error()) {
		t.Error("[Promote] wrong error promoting not existed fs")
	}
}

func TestSendReceive(t *testing.T) {
	srcFs, err := CreateFs(testPath + "/src")
	if err != nil {
		t.Fatal("[SndRcv] error creating fs:", err)
	}
	defer srcFs.Destroy(RF_Hard)

	srcSnap, err := srcFs.Snapshot("s1")
	if err != nil {
		t.Fatal("[SndRcv] error creating fs:", err)
	}

	srcSize, err := srcFs.GetProperty("usedbydataset")
	if err != nil {
		t.Fatal("[SndRcv] error creating fs:", err)
	}

	destFs := NewFs(sendPath + "/dest")

	err = srcSnap.Send(destFs)
	if err != nil {
		t.Error("[SndRcv] error sending snapshot:", err)
	}

	if ok, _ := destFs.Exists(); !ok {
		t.Error("[SndRcv] destination fs doesn't exists")
	}

	destSize, _ := destFs.GetProperty("usedbydataset")

	if srcSize != destSize {
		t.Errorf("[SndRcv] dest fs have different size %s, wanted %s",
			destSize, srcSize)
	}

	destSnap := NewSnapshot(destFs.Path + "@s1")
	if ok, _ := destSnap.Exists(); !ok {
		t.Error("[SndRcv] destination snapshot fs doesn't exists")
	}

	secondSnap, _ := srcFs.Snapshot("s2")
	err = secondSnap.SendIncremental(srcSnap, destFs)
	if err != nil {
		t.Error("[SndRcv] error sending incremental snapshot:", err)
	}

	destSnap = NewSnapshot(destFs.Path + "@s2")
	if ok, _ := destSnap.Exists(); !ok {
		t.Error("[SndRcv] destination snapshot fs doesn't exists after incremental")
	}

	srcFs.Destroy(RF_Hard)
	destFs.Destroy(RF_Hard)

	fmt.Println("Sending not existing fs")
	srcSnap = NewSnapshot(unicorn + "@s1")
	err = srcSnap.Send(destFs)
	if err == nil {
		t.Error("[SndRcv] sended not existent snapshot without errors")
	}
	if !NotExist.MatchString(err.Error()) {
		t.Error("[SndRcv] wrong error sending not existent snapshot:", err)
	}

	srcFs, err = CreateFs(testPath + "/src")
	if err != nil {
		t.Fatal("[SndRcv] error creating src fs:", err)
	}
	t.Log("created source Fs")
	srcSnap, err = srcFs.Snapshot("s1")
	if err != nil {
		t.Fatal("[SndRcv] error creating src fs snapshot:", err)
	}
	t.Log("created source Fs snapshot")

	fss, _ := ListFs(testPath)
	t.Log("filesystems: %s", fss)
	snaps, _ := srcFs.ListSnapshots()
	t.Log("snapshots: %s", snaps)

	destFs = NewFs(badDataset)
	fmt.Println("Sending to bad fs")

	err = srcSnap.Send(destFs)
	if err == nil {
		destFs.Destroy(RF_No)
		t.Error("[SndRcv] sended to bad fs")
	}

	if !BrokenPipe.MatchString(err.Error()) {
		t.Fatal("[SndRcv] wrong error sending to bad dataset:", err)
	}

	err = srcSnap.Send(srcFs)
	if err == nil {
		t.Error("[SndRcv] sended to existent fs")
	}
	if !BrokenPipe.MatchString(err.Error()) {
		t.Error("[SndRcv] wrong error sending to existent fs:", err)
	}
	srcFs.Destroy(RF_Hard)
}

func TestRemote(t *testing.T) {
	r, err := runcmd.NewRemotePassAuthRunner(user, "localhost:22", pass)
	if err != nil {
		t.Fatal("[Remote] error initializing connection:", err)
	}

	z := NewZfs(r, true)
	fs, err := z.CreateFs(testPath + "/fs")
	if err != nil {
		t.Error("[Remote]", err)
	}

	fs.Destroy(RF_No)
}
