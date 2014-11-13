package zfs

import (
	"fmt"

	"github.com/theairkit/runcmd"
)
import "testing"

const (
	testPath   string = "tank/test"
	sendPath   string = testPath
	otherPool  string = "zssd/test"
	sudoPath   string = "tank/sudo"
	unicorn    string = testPath + "/unicorn"
	badDataset string = testPath + "/bad/"
	user       string = "persienko"
	pass       string = "PASSWORD"
)

func TestGetPool(t *testing.T) {
	pool := NewFs("tank/some/thing").GetPool()
	if pool != "tank" {
		t.Error("GetPool: Wrong pool")
	}
}

func TestGetLastPath(t *testing.T) {
	pool := NewFs("tank/some/thing").GetLastPath()
	if pool != "thing" {
		t.Error("LastPath: Wrong last name")
	}
}

func TestExists(t *testing.T) {
	ok, err := NewFs(testPath).Exists()
	if err != nil {
		t.Error("Exists:", err)
	}

	if !ok {
		t.Errorf("Exists: %s exists, but returned false", testPath)
	}

	ok, err = NewFs(unicorn).Exists()
	if err != nil {
		t.Error(err)
	}

	if ok {
		t.Error("Exists: unicorns doesn't exists, but returned true")
	}

	ok, err = NewFs(badDataset).Exists()
	if ok {
		t.Error("Exists: returned true on bad dataset")
	}
	if !InvalidDataset.MatchString(err.Error()) {
		t.Error("Exists: wrong error checking invalid dataset:", err)
	}
}

func TestCreateFs(t *testing.T) {
	fs, err := CreateFs(testPath + "/fs1")
	if err != nil {
		t.Fatal("CreateFs:", err)
	}
	if ok, _ := fs.Exists(); !ok {
		t.Error("CreateFs: fs not created!")
	}

	_, err = CreateFs(testPath + "/fs1")
	if err == nil {
		t.Error("CreateFs: created allready existed fs")
	}
	if !AllreadyExists.MatchString(err.Error()) {
		t.Error("CreateFs: wrong error creating dup fs:", err)
	}
	fs.Destroy(false)

	_, err = CreateFs(badDataset)
	if err == nil {
		t.Error("CreateFs: created fs with bad name")
	}
	if !InvalidDataset.MatchString(err.Error()) {
		t.Error("CreateFs: wrong error while creating fs with bad name:", err)
	}
}

func TestGetProperty(t *testing.T) {
	fs, _ := CreateFs(testPath + "/fs1")
	ty, err := fs.GetProperty("type")
	if err != nil {
		t.Error("GetProp: error getting property:", err)
	}

	if ty != "filesystem" {
		t.Error("GetProp: returned wrong value for 'type': %s, want filesystem",
			t)
	}

	_, err = fs.GetProperty("notexist")
	if err == nil {
		t.Error("GetProp: got not existent property")
	}
	if !BadPropGet.MatchString(err.Error()) {
		t.Error("GetProp: wrong error getting bad property:", err)
	}

	fs.Destroy(false)
}

func TestSetProperty(t *testing.T) {
	fs, _ := CreateFs(testPath + "/fs1")
	err := fs.SetProperty("quota", "1000000")
	if err != nil {
		t.Error("SetProperty: error setting property:", err)
	}

	err = fs.SetProperty("oki", "doki")
	if err == nil {
		t.Error("SetProperty: set bad property")
	}
	if !BadPropSet.MatchString(err.Error()) {
		t.Error("SetProperty: wrong error setting bad property:", err)
	}

	fs.Destroy(false)
}

func TestSnapshot(t *testing.T) {
	fs, _ := CreateFs(testPath + "/fs1")
	s, err := fs.Snapshot("s1")
	if err != nil {
		t.Error("Snapshot: error creating snapshot:", err)
	}

	spath := testPath + "/fs1@s1"
	if s.Path != spath {
		t.Error("Snapshot: wrong snapshot path: %s, wanted: %s", s.Path, spath)
	}
	if s.Name != "s1" {
		t.Error("Snapshot: wrong snapshot name: %s, wanted: %s", s.Name, "s1")
	}
	if s.Fs.Path != testPath+"/fs1" {
		t.Error("Snapshot: wrong snapshot fs path: %s, wanted: %s",
			s.Fs.Path, testPath+"/fs1")
	}

	if ok, _ := s.Exists(); !ok {
		t.Error("Snapshot: snapshot not created")
	}

	if ok, _ := NewSnapshot(testPath + "/fs1@s1").Exists(); !ok {
		t.Error("Snapshot: NewSnapshot not works...")
	}

	fs.Destroy(true)

	_, err = NewFs(unicorn).Snapshot("s2")
	if err == nil {
		t.Error("Snapshot: created snapshot on not existent fs")
	}
	if !NotExist.MatchString(err.Error()) {
		t.Error("Snapshot: wrong error creating snap on unicorn:", err)
	}
}

func TestDestroyFs(t *testing.T) {
	fs, _ := CreateFs(testPath + "/fs5")

	err := fs.Destroy(false)
	if err != nil {
		t.Error("DestroyFs:", err)
	}

	ok, _ := fs.Exists()
	if ok {
		t.Error("DestroyFs: fs not deleted")
	}

	// Destroy invalid Dataset
	err = NewFs(badDataset).Destroy(false)
	if !InvalidDataset.MatchString(err.Error()) {
		t.Error("DestroyFs: wrong error deleting invalid dataset:", err)
	}
}

func TestDestroyRecursive(t *testing.T) {
	fs, _ := CreateFs(testPath + "/fs1")
	snap, _ := fs.Snapshot("s1")

	err := fs.Destroy(false)
	if err == nil {
		t.Error("Destroyed fs with snapshot without recursive flag")
	}

	err = fs.Destroy(true)
	if err != nil {
		t.Error(err)
	}

	ok, _ := fs.Exists()
	if ok {
		t.Error("fs not deleted")
	}

	ok, _ = snap.Exists()
	if ok {
		t.Error("snapshot not deleted")
	}

	_, err = NewFs(badDataset).Snapshot("test")
	if err == nil {
		t.Error("Snapshot: created snapshot for bad fs")
	}

	if !InvalidDataset.MatchString(err.Error()) {
		t.Error("Snapshot: wrong error while creating snapshot for bad fs:", err)
	}
}

func TestListFs(t *testing.T) {
	want := []string{"", "/fs1", "/fs2", "/fs2/fs3"}

	for _, f := range want[1:] {
		CreateFs(testPath + f)
	}

	fs, err := ListFs(testPath)
	if err != nil {
		t.Errorf("ListFs: %s", err)
	}

	if len(fs) != len(want) {
		t.Fatal("ListFs: fs size differs from wanted")
	}
	for i, fs := range fs {
		if fs.Path != testPath+want[i] {
			t.Error("ListFs: fs %s differs from wanted (%s)", fs.Path, want[i])
		}
		if want[i] != "" {
			fs.Destroy(true)
		}
	}

	fs, err = ListFs(testPath + "/magic/forest")
	if err != nil {
		t.Error("ListFs:", err)
	}

	if len(fs) > 0 {
		t.Error("ListFs: found something in magic forest, but it doesn't exists!")
	}

	fs, err = ListFs(badDataset)
	if len(fs) != 0 {
		t.Error("ListFs: returned not empty fs list on bad dataset:", fs)
	}
	if !InvalidDataset.MatchString(err.Error()) {
		t.Error("ListFs: wrong error checking invalid dataset:", err)
	}
}

func TestListSnapshots(t *testing.T) {
	fs, _ := CreateFs(testPath + "/fs1")
	fs.Snapshot("s1")
	fs.Snapshot("s2")

	want := []string{"/fs1@s1", "/fs1@s2"}
	snaps, err := fs.ListSnapshots()
	if err != nil {
		t.Error("ListSnapshots:", err)
	}

	for i, snap := range snaps {
		if snap.Path != testPath+want[i] {
			t.Errorf("ListSnapshots: fs %s differs from wanted (%s)",
				snap.Path, want[i])
		}
	}

	fs.Destroy(true)

	_, err = NewFs(badDataset).ListSnapshots()
	if err == nil {
		t.Error("ListSnapshots: listed bad fs")
	}

	if !InvalidDataset.MatchString(err.Error()) {
		t.Error("ListSnapshots: wrong error while listing snapshots for bad fs:", err)
	}
}

func TestSudo(t *testing.T) {
	fs, err := CreateFs(sudoPath + "/fs1")
	if err == nil {
		t.Error("Sudo: created without sudo")
	}
	if !NotMounted.MatchString(err.Error()) {
		t.Error("Sudo: wrong error:", err)
	}
	fs.Destroy(false)

	err = SetStdSudo(true)
	if err != nil {
		t.Error("Sudo: error switching sudo on:", err)
	}

	fs, err = CreateFs(sudoPath + "/fs2")
	if err != nil {
		t.Error("Sudo: error creating fs with sudo:", err)
	}

	fs, err = CreateFs(sudoPath + "/fs1")
	if err != nil {
		t.Error("Sudo: error creating fs:", err)
	}
	err = fs.Destroy(false)
	if err != nil {
		t.Error("Sudo: error destroying fs with sudo:", err)
	}

	err = SetStdSudo(false)
	if err != nil {
		t.Error("Sudo: error switching sudo off:", err)
	}

	err = NewFs(sudoPath + "/fs2").Destroy(false)
	if err == nil {
		t.Error("Sudo: sudo doesn't switch off")
	}

	SetStdSudo(true)
	NewFs(sudoPath + "/fs2").Destroy(false)
}

func TestClone(t *testing.T) {
	fs, _ := CreateFs(testPath + "/fs1")
	sn, _ := fs.Snapshot("s1")

	cl, err := sn.Clone(testPath + "/fs2")
	if err != nil {
		t.Error("Clone: error creating clone:", err)
	}

	cl.Destroy(false)

	_, err = sn.Clone(testPath + "@qa")
	if err == nil {
		t.Error("Clone: created clone with bad name")
	}

	fs.Destroy(true)

	_, err = sn.Clone(otherPool + "/fs2")
	if err == nil {
		t.Error("Clone: created clone on other pool")
	}
	if err != PoolError {
		t.Errorf("Clone: wrong error: %s, want %s", err, PoolError)
	}
}

func TestPromote(t *testing.T) {
	origFs, _ := CreateFs(testPath + "/fs6")
	snap, _ := origFs.Snapshot("s1")
	clone, _ := snap.Clone(testPath + "/fs7")

	newSnap := clone.Path + "@s1"

	err := clone.Promote()
	if err != nil {
		t.Fatal("Promote: errors while promoting:", err)
	}

	origin, _ := origFs.GetProperty("origin")
	if origin != newSnap {
		t.Errorf("Promote: origin have wrong owner %s, want %s", origin, newSnap)
	}

	err = clone.Promote()
	if err == nil {
		t.Error("Promote: promoted not clone fs")
	}
	if !PromoteNotClone.MatchString(err.Error()) {
		t.Error("Promote: wrong error promoting not clone fs:", err)
	}

	NewFs(testPath + "/fs6").Destroy(false)
	NewFs(testPath + "/fs7").Destroy(true)

	err = NewFs(unicorn).Promote()

	if err == nil {
		t.Error("Promote: promoted not existed fs")
	}
	if !NotExist.MatchString(err.Error()) {
		t.Error("Promote: wrong error promoting not existed fs")
	}
}

func TestSendReceive(t *testing.T) {
	srcFs, _ := CreateFs(testPath + "/src")
	srcSnap, _ := srcFs.Snapshot("s1")
	srcSize, _ := srcFs.GetProperty("usedbydataset")

	destFs := NewFs(sendPath + "/dest")

	err := srcSnap.Send(destFs)
	if err != nil {
		t.Error("SndRcv: error sending snapshot:", err)
	}

	if ok, _ := destFs.Exists(); !ok {
		t.Error("SndRcv: destination fs doesn't exists")
	}

	destSize, _ := destFs.GetProperty("usedbydataset")

	if srcSize != destSize {
		t.Errorf("SndRcv: dest fs have different size %s, wanted %s",
			destSize, srcSize)
	}

	destSnap := NewSnapshot(destFs.Path + "@s1")
	if ok, _ := destSnap.Exists(); !ok {
		t.Error("SndRcv: destination snapshot fs doesn't exists")
	}

	secondSnap, _ := srcFs.Snapshot("s2")
	err = secondSnap.SendInc(srcSnap, destFs)
	if err != nil {
		t.Error("SndRcv: error sending incremental snapshot:", err)
	}

	destSnap = NewSnapshot(destFs.Path + "@s2")
	if ok, _ := destSnap.Exists(); !ok {
		t.Error("SndRcv: destination snapshot fs doesn't exists after incremental")
	}

	srcFs.Destroy(true)
	destFs.Destroy(true)

	fmt.Println("Sending not existing fs")
	srcSnap = NewSnapshot(unicorn + "@s1")
	err = srcSnap.Send(destFs)
	if err == nil {
		t.Error("SndRcv: sended not existent snapshot without errors")
	}
	if !NotExist.MatchString(err.Error()) {
		t.Error("SndRcv: wrong error sending not existent snapshot:", err)
	}

	srcFs, _ = CreateFs(testPath + "/src")
	srcSnap, _ = srcFs.Snapshot("s1")

	destFs = NewFs(badDataset)
	fmt.Println("Sending to bad fs")
	err = srcSnap.Send(destFs)
	if err == nil {
		t.Error("SndRcv: sended to bad fs")
	}
	if !InvalidDataset.MatchString(err.Error()) {
		t.Error("SndRcv: wrong error sending to bad dataset:", err)
	}

	err = srcSnap.Send(srcFs)
	if err == nil {
		t.Error("SndRcv: sended to existent fs")
	}
	if !ReceiverExists.MatchString(err.Error()) {
		t.Error("SndRcv: wrong error sending to existent fs:", err)
	}
	srcFs.Destroy(true)
}

func TestRemote(t *testing.T) {
	r, err := runcmd.NewRemotePassAuthRunner(user, "localhost:22", pass)
	if err != nil {
		t.Fatal("Remote: error initializing connection:", err)
	}

	z := NewZfs(r, false)
	fs, err := z.CreateFs(testPath + "/fs")
	if err != nil {
		t.Error("Reomte:", err)
	}

	fs.Destroy(false)
}
