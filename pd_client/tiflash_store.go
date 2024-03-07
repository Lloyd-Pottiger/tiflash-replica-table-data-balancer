package pdclient

type TiFlashStoreRegionSet struct {
	ID          int64
	RegionIDSet map[int64]struct{}
}
