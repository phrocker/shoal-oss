package metadata

// System table IDs. From core/.../metadata/SystemTables.java.
const (
	RootTableID     = "+r" // SystemTables.ROOT
	MetadataTableID = "!0" // SystemTables.METADATA
)

// Column-family + qualifier names from core/.../metadata/schema/MetadataSchema.java.
const (
	CFFile            = "file"   // DataFileColumnFamily.STR_NAME
	CFCurrentLocation = "loc"    // CurrentLocationColumnFamily.STR_NAME
	CFFutureLocation  = "future" // FutureLocationColumnFamily.STR_NAME
	CFTabletSection   = "~tab"   // TabletColumnFamily — holds prev-row etc.
	CQPrevRow         = "~pr"    // PrevRowColumn qualifier under ~tab
)
