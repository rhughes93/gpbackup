package integration

import (
	"sort"

	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup integration tests", func() {
	Describe("GetAllUserTables", func() {
		BeforeEach(func() {
			backup.SetLeafPartitionData(false)
		})
		It("returns user table information for basic heap tables", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE foo(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE foo")
			testutils.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
			defer testutils.AssertQueryRuns(connection, "DROP SCHEMA testschema CASCADE")
			testutils.AssertQueryRuns(connection, "CREATE TABLE testschema.testtable(t text)")

			tables := backup.GetAllUserTables(connection)

			tableFoo := backup.BasicRelation("public", "foo")
			tableTestTable := backup.BasicRelation("testschema", "testtable")

			Expect(len(tables)).To(Equal(2))
			testutils.ExpectStructsToMatchExcluding(&tableFoo, &tables[0], "SchemaOid", "Oid")
			testutils.ExpectStructsToMatchExcluding(&tableTestTable, &tables[1], "SchemaOid", "Oid")
		})
		Context("Retrieving external partitions", func() {
			It("returns parent and external leaf partition table if the filter includes a leaf table and leaf-partition-data is set", func() {
				backup.SetLeafPartitionData(true)
				backup.SetIncludeTables([]string{"public.partition_table_1_prt_boys"})
				defer backup.SetIncludeTables([]string{})
				testutils.AssertQueryRuns(connection, `CREATE TABLE partition_table (id int, gender char(1))
DISTRIBUTED BY (id)
PARTITION BY LIST (gender)
( PARTITION girls VALUES ('F'),
  PARTITION boys VALUES ('M'),
  DEFAULT PARTITION other );`)
				testutils.AssertQueryRuns(connection, `CREATE EXTERNAL WEB TABLE partition_table_ext_part_ (like partition_table_1_prt_girls)
EXECUTE 'echo -e "2\n1"' on host
FORMAT 'csv';`)
				testutils.AssertQueryRuns(connection, `ALTER TABLE public.partition_table EXCHANGE PARTITION girls WITH TABLE public.partition_table_ext_part_ WITHOUT VALIDATION;`)
				defer testutils.AssertQueryRuns(connection, "DROP TABLE partition_table")
				defer testutils.AssertQueryRuns(connection, "DROP TABLE partition_table_ext_part_")

				tables := backup.GetAllUserTables(connection)

				expectedTableNames := []string{"public.partition_table", "public.partition_table_1_prt_boys", "public.partition_table_1_prt_girls"}
				tableNames := make([]string, 0)
				for _, table := range tables {
					tableNames = append(tableNames, table.FQN())
				}
				sort.Strings(tableNames)

				Expect(len(tables)).To(Equal(3))
				Expect(tableNames).To(Equal(expectedTableNames))
			})
			It("returns external partition tables for an included parent table if the filter includes a parent partition table", func() {
				backup.SetIncludeTables([]string{"public.partition_table1", "public.partition_table2_1_prt_other"})
				defer backup.SetIncludeTables([]string{})
				testutils.AssertQueryRuns(connection, `CREATE TABLE partition_table1 (id int, gender char(1))
DISTRIBUTED BY (id)
PARTITION BY LIST (gender)
( PARTITION girls VALUES ('F'),
  PARTITION boys VALUES ('M'),
  DEFAULT PARTITION other );`)
				testutils.AssertQueryRuns(connection, `CREATE EXTERNAL WEB TABLE partition_table1_ext_part_ (like partition_table1_1_prt_boys)
EXECUTE 'echo -e "2\n1"' on host
FORMAT 'csv';`)
				testutils.AssertQueryRuns(connection, `ALTER TABLE public.partition_table1 EXCHANGE PARTITION boys WITH TABLE public.partition_table1_ext_part_ WITHOUT VALIDATION;`)
				defer testutils.AssertQueryRuns(connection, "DROP TABLE partition_table1")
				defer testutils.AssertQueryRuns(connection, "DROP TABLE partition_table1_ext_part_")
				testutils.AssertQueryRuns(connection, `CREATE TABLE partition_table2 (id int, gender char(1))
DISTRIBUTED BY (id)
PARTITION BY LIST (gender)
( PARTITION girls VALUES ('F'),
  PARTITION boys VALUES ('M'),
  DEFAULT PARTITION other );`)
				testutils.AssertQueryRuns(connection, `CREATE EXTERNAL WEB TABLE partition_table2_ext_part_ (like partition_table2_1_prt_girls)
EXECUTE 'echo -e "2\n1"' on host
FORMAT 'csv';`)
				testutils.AssertQueryRuns(connection, `ALTER TABLE public.partition_table2 EXCHANGE PARTITION girls WITH TABLE public.partition_table2_ext_part_ WITHOUT VALIDATION;`)
				defer testutils.AssertQueryRuns(connection, "DROP TABLE partition_table2")
				defer testutils.AssertQueryRuns(connection, "DROP TABLE partition_table2_ext_part_")
				testutils.AssertQueryRuns(connection, `CREATE TABLE partition_table3 (id int, gender char(1))
DISTRIBUTED BY (id)
PARTITION BY LIST (gender)
( PARTITION girls VALUES ('F'),
  PARTITION boys VALUES ('M'),
  DEFAULT PARTITION other );`)
				testutils.AssertQueryRuns(connection, `CREATE EXTERNAL WEB TABLE partition_table3_ext_part_ (like partition_table3_1_prt_girls)
EXECUTE 'echo -e "2\n1"' on host
FORMAT 'csv';`)
				testutils.AssertQueryRuns(connection, `ALTER TABLE public.partition_table3 EXCHANGE PARTITION girls WITH TABLE public.partition_table3_ext_part_ WITHOUT VALIDATION;`)
				defer testutils.AssertQueryRuns(connection, "DROP TABLE partition_table3")
				defer testutils.AssertQueryRuns(connection, "DROP TABLE partition_table3_ext_part_")

				tables := backup.GetAllUserTables(connection)

				expectedTableNames := []string{"public.partition_table1", "public.partition_table1_1_prt_boys", "public.partition_table2", "public.partition_table2_1_prt_girls", "public.partition_table2_1_prt_other"}
				tableNames := make([]string, 0)
				for _, table := range tables {
					tableNames = append(tableNames, table.FQN())
				}
				sort.Strings(tableNames)

				Expect(len(tables)).To(Equal(5))
				Expect(tableNames).To(Equal(expectedTableNames))
			})
		})
		Context("leaf-partition-data flag", func() {
			It("returns only parent partition tables if the leaf-partition-data flag is not set and there are no include tables", func() {
				createStmt := `CREATE TABLE rank (id int, rank int, year int, gender
char(1), count int )
DISTRIBUTED BY (id)
PARTITION BY LIST (gender)
( PARTITION girls VALUES ('F'),
  PARTITION boys VALUES ('M'),
  DEFAULT PARTITION other );`
				testutils.AssertQueryRuns(connection, createStmt)
				defer testutils.AssertQueryRuns(connection, "DROP TABLE rank")

				tables := backup.GetAllUserTables(connection)

				tableRank := backup.BasicRelation("public", "rank")

				Expect(len(tables)).To(Equal(1))
				testutils.ExpectStructsToMatchExcluding(&tableRank, &tables[0], "SchemaOid", "Oid")
			})
			It("returns both parent and leaf partition tables if the leaf-partition-data flag is set and there are no include tables", func() {
				backup.SetLeafPartitionData(true)
				createStmt := `CREATE TABLE rank (id int, rank int, year int, gender
char(1), count int )
DISTRIBUTED BY (id)
PARTITION BY LIST (gender)
( PARTITION girls VALUES ('F'),
  PARTITION boys VALUES ('M'),
  DEFAULT PARTITION other );`
				testutils.AssertQueryRuns(connection, createStmt)
				defer testutils.AssertQueryRuns(connection, "DROP TABLE rank")

				tables := backup.GetAllUserTables(connection)

				expectedTableNames := []string{"public.rank", "public.rank_1_prt_boys", "public.rank_1_prt_girls", "public.rank_1_prt_other"}
				tableNames := make([]string, 0)
				for _, table := range tables {
					tableNames = append(tableNames, table.FQN())
				}
				sort.Strings(tableNames)

				Expect(len(tables)).To(Equal(4))
				Expect(tableNames).To(Equal(expectedTableNames))
			})
			It("returns parent and included child partition table if the filter includes a leaf table; with and without leaf-partition-data", func() {
				backup.SetIncludeTables([]string{"public.rank_1_prt_girls"})
				defer backup.SetIncludeTables([]string{})
				createStmt := `CREATE TABLE rank (id int, rank int, year int, gender
char(1), count int )
DISTRIBUTED BY (id)
PARTITION BY LIST (gender)
( PARTITION girls VALUES ('F'),
  PARTITION boys VALUES ('M'),
  DEFAULT PARTITION other );`
				testutils.AssertQueryRuns(connection, createStmt)
				defer testutils.AssertQueryRuns(connection, "DROP TABLE rank")

				expectedTableNames := []string{"public.rank", "public.rank_1_prt_girls"}

				tables := backup.GetAllUserTables(connection)
				tableNames := make([]string, 0)
				for _, table := range tables {
					tableNames = append(tableNames, table.FQN())
				}
				sort.Strings(tableNames)

				Expect(len(tables)).To(Equal(2))
				Expect(tableNames).To(Equal(expectedTableNames))

				backup.SetLeafPartitionData(true)
				tables = backup.GetAllUserTables(connection)
				tableNames = make([]string, 0)
				for _, table := range tables {
					tableNames = append(tableNames, table.FQN())
				}
				sort.Strings(tableNames)

				Expect(len(tables)).To(Equal(2))
				Expect(tableNames).To(Equal(expectedTableNames))
			})
			It("returns child partition tables for an included parent table if the leaf-partition-data flag is set and the filter includes a parent partition table", func() {
				backup.SetLeafPartitionData(true)
				backup.SetIncludeTables([]string{"public.rank"})
				defer backup.SetIncludeTables([]string{})
				createStmt := `CREATE TABLE rank (id int, rank int, year int, gender
char(1), count int )
DISTRIBUTED BY (id)
PARTITION BY LIST (gender)
( PARTITION girls VALUES ('F'),
  PARTITION boys VALUES ('M'),
  DEFAULT PARTITION other );`
				testutils.AssertQueryRuns(connection, createStmt)
				defer testutils.AssertQueryRuns(connection, "DROP TABLE rank")
				testutils.AssertQueryRuns(connection, "CREATE TABLE test_table(i int)")
				defer testutils.AssertQueryRuns(connection, "DROP TABLE test_table")

				tables := backup.GetAllUserTables(connection)

				expectedTableNames := []string{"public.rank", "public.rank_1_prt_boys", "public.rank_1_prt_girls", "public.rank_1_prt_other"}
				tableNames := make([]string, 0)
				for _, table := range tables {
					tableNames = append(tableNames, table.FQN())
				}
				sort.Strings(tableNames)

				Expect(len(tables)).To(Equal(4))
				Expect(tableNames).To(Equal(expectedTableNames))
			})
		})
		It("returns user table information for table in specific schema", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE foo(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE foo")
			testutils.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
			defer testutils.AssertQueryRuns(connection, "DROP SCHEMA testschema")
			testutils.AssertQueryRuns(connection, "CREATE TABLE testschema.foo(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE testschema.foo")

			backup.SetIncludeSchemas([]string{"testschema"})
			tables := backup.GetAllUserTables(connection)

			tableFoo := backup.BasicRelation("testschema", "foo")

			Expect(len(tables)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&tableFoo, &tables[0], "SchemaOid", "Oid")
		})
		It("returns user table information for tables in includeTables", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE foo(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE foo")
			testutils.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
			defer testutils.AssertQueryRuns(connection, "DROP SCHEMA testschema")
			testutils.AssertQueryRuns(connection, "CREATE TABLE testschema.foo(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE testschema.foo")

			backup.SetIncludeTables([]string{"testschema.foo"})
			tables := backup.GetAllUserTables(connection)

			tableFoo := backup.BasicRelation("testschema", "foo")

			Expect(len(tables)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&tableFoo, &tables[0], "SchemaOid", "Oid")
		})
		It("returns user table information for tables not in excludeTables", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE foo(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE foo")
			testutils.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
			defer testutils.AssertQueryRuns(connection, "DROP SCHEMA testschema")
			testutils.AssertQueryRuns(connection, "CREATE TABLE testschema.foo(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE testschema.foo")

			backup.SetExcludeTables([]string{"testschema.foo"})
			tables := backup.GetAllUserTables(connection)

			tableFoo := backup.BasicRelation("public", "foo")

			Expect(len(tables)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&tableFoo, &tables[0], "SchemaOid", "Oid")
		})
		It("returns user table information for tables in includeSchema but not in excludeTables", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE foo(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE foo")
			testutils.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
			defer testutils.AssertQueryRuns(connection, "DROP SCHEMA testschema")
			testutils.AssertQueryRuns(connection, "CREATE TABLE testschema.foo(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE testschema.foo")
			testutils.AssertQueryRuns(connection, "CREATE TABLE testschema.bar(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE testschema.bar")

			backup.SetIncludeSchemas([]string{"testschema"})
			backup.SetExcludeTables([]string{"testschema.foo"})
			tables := backup.GetAllUserTables(connection)

			tableFoo := backup.BasicRelation("testschema", "bar")

			Expect(len(tables)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&tableFoo, &tables[0], "SchemaOid", "Oid")
		})
	})
	Describe("GetPartitionTableMap", func() {
		It("correctly maps oids to parent or leaf table types", func() {
			createStmt := `CREATE TABLE summer_sales (id int, year int, month int)
DISTRIBUTED BY (id)
PARTITION BY RANGE (year)
    SUBPARTITION BY RANGE (month)
       SUBPARTITION TEMPLATE (
        START (6) END (8) EVERY (1),
        DEFAULT SUBPARTITION other_months )
( START (2015) END (2017) EVERY (1),
  DEFAULT PARTITION outlying_years );
`
			testutils.AssertQueryRuns(connection, createStmt)
			defer testutils.AssertQueryRuns(connection, "DROP TABLE summer_sales")

			parent := testutils.OidFromObjectName(connection, "public", "summer_sales", backup.TYPE_RELATION)
			intermediate1 := testutils.OidFromObjectName(connection, "public", "summer_sales_1_prt_outlying_years", backup.TYPE_RELATION)
			leaf11 := testutils.OidFromObjectName(connection, "public", "summer_sales_1_prt_outlying_years_2_prt_2", backup.TYPE_RELATION)
			leaf12 := testutils.OidFromObjectName(connection, "public", "summer_sales_1_prt_outlying_years_2_prt_3", backup.TYPE_RELATION)
			leaf13 := testutils.OidFromObjectName(connection, "public", "summer_sales_1_prt_outlying_years_2_prt_other_months", backup.TYPE_RELATION)
			intermediate2 := testutils.OidFromObjectName(connection, "public", "summer_sales_1_prt_2", backup.TYPE_RELATION)
			leaf21 := testutils.OidFromObjectName(connection, "public", "summer_sales_1_prt_2_2_prt_2", backup.TYPE_RELATION)
			leaf22 := testutils.OidFromObjectName(connection, "public", "summer_sales_1_prt_2_2_prt_3", backup.TYPE_RELATION)
			leaf23 := testutils.OidFromObjectName(connection, "public", "summer_sales_1_prt_2_2_prt_other_months", backup.TYPE_RELATION)
			intermediate3 := testutils.OidFromObjectName(connection, "public", "summer_sales_1_prt_3", backup.TYPE_RELATION)
			leaf31 := testutils.OidFromObjectName(connection, "public", "summer_sales_1_prt_3_2_prt_2", backup.TYPE_RELATION)
			leaf32 := testutils.OidFromObjectName(connection, "public", "summer_sales_1_prt_3_2_prt_3", backup.TYPE_RELATION)
			leaf33 := testutils.OidFromObjectName(connection, "public", "summer_sales_1_prt_3_2_prt_other_months", backup.TYPE_RELATION)
			partTableMap := backup.GetPartitionTableMap(connection)

			Expect(len(partTableMap)).To(Equal(13))
			Expect(partTableMap[parent]).To(Equal("p"))
			Expect(partTableMap[intermediate1]).To(Equal("i"))
			Expect(partTableMap[intermediate2]).To(Equal("i"))
			Expect(partTableMap[intermediate3]).To(Equal("i"))
			Expect(partTableMap[leaf11]).To(Equal("l"))
			Expect(partTableMap[leaf12]).To(Equal("l"))
			Expect(partTableMap[leaf13]).To(Equal("l"))
			Expect(partTableMap[leaf21]).To(Equal("l"))
			Expect(partTableMap[leaf22]).To(Equal("l"))
			Expect(partTableMap[leaf23]).To(Equal("l"))
			Expect(partTableMap[leaf31]).To(Equal("l"))
			Expect(partTableMap[leaf32]).To(Equal("l"))
			Expect(partTableMap[leaf33]).To(Equal("l"))
		})
	})
	Describe("GetColumnDefinitions", func() {
		It("returns table attribute information for a heap table", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE atttable(a float, b text, c text NOT NULL, d int DEFAULT(5), e text)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE atttable")
			testutils.AssertQueryRuns(connection, "COMMENT ON COLUMN atttable.a IS 'att comment'")
			testutils.AssertQueryRuns(connection, "ALTER TABLE atttable DROP COLUMN b")
			testutils.AssertQueryRuns(connection, "ALTER TABLE atttable ALTER COLUMN e SET STORAGE PLAIN")
			oid := testutils.OidFromObjectName(connection, "public", "atttable", backup.TYPE_RELATION)

			tableAtts := backup.GetColumnDefinitions(connection)[oid]

			columnA := backup.ColumnDefinition{Oid: 0, Num: 1, Name: "a", NotNull: false, HasDefault: false, Type: "double precision", Encoding: "", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: "att comment"}
			columnC := backup.ColumnDefinition{Oid: 0, Num: 3, Name: "c", NotNull: true, HasDefault: false, Type: "text", Encoding: "", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: ""}
			columnD := backup.ColumnDefinition{Oid: 0, Num: 4, Name: "d", NotNull: false, HasDefault: true, Type: "integer", Encoding: "", StatTarget: -1, StorageType: "", DefaultVal: "5", Comment: ""}
			columnE := backup.ColumnDefinition{Oid: 0, Num: 5, Name: "e", NotNull: false, HasDefault: false, Type: "text", Encoding: "", StatTarget: -1, StorageType: "PLAIN", DefaultVal: "", Comment: ""}

			Expect(len(tableAtts)).To(Equal(4))

			testutils.ExpectStructsToMatchExcluding(&columnA, &tableAtts[0], "Oid")
			testutils.ExpectStructsToMatchExcluding(&columnC, &tableAtts[1], "Oid")
			testutils.ExpectStructsToMatchExcluding(&columnD, &tableAtts[2], "Oid")
			testutils.ExpectStructsToMatchExcluding(&columnE, &tableAtts[3], "Oid")
		})
		It("returns table attributes including encoding for a column oriented table", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE co_atttable(a float, b text ENCODING(blocksize=65536)) WITH (appendonly=true, orientation=column)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE co_atttable")
			oid := testutils.OidFromObjectName(connection, "public", "co_atttable", backup.TYPE_RELATION)

			tableAtts := backup.GetColumnDefinitions(connection)[oid]

			columnA := backup.ColumnDefinition{Oid: 0, Num: 1, Name: "a", NotNull: false, HasDefault: false, Type: "double precision", Encoding: "compresstype=none,blocksize=32768,compresslevel=0", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: ""}
			columnB := backup.ColumnDefinition{Oid: 0, Num: 2, Name: "b", NotNull: false, HasDefault: false, Type: "text", Encoding: "blocksize=65536,compresstype=none,compresslevel=0", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: ""}

			Expect(len(tableAtts)).To(Equal(2))

			testutils.ExpectStructsToMatchExcluding(&columnA, &tableAtts[0], "Oid")
			testutils.ExpectStructsToMatchExcluding(&columnB, &tableAtts[1], "Oid")
		})
		It("returns an empty attribute array for a table with no columns", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE nocol_atttable()")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE nocol_atttable")
			oid := testutils.OidFromObjectName(connection, "public", "nocol_atttable", backup.TYPE_RELATION)

			tableAtts := backup.GetColumnDefinitions(connection)[oid]

			Expect(len(tableAtts)).To(Equal(0))
		})
	})
	Describe("GetDistributionPolicies", func() {
		It("returns distribution policy info for a table DISTRIBUTED RANDOMLY", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE dist_random(a int, b text) DISTRIBUTED RANDOMLY")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE dist_random")
			oid := testutils.OidFromObjectName(connection, "public", "dist_random", backup.TYPE_RELATION)

			tables := []backup.Relation{{Oid: oid}}
			distPolicies := backup.GetDistributionPolicies(connection, tables)[oid]

			Expect(distPolicies).To(Equal("DISTRIBUTED RANDOMLY"))
		})
		It("returns distribution policy info for a table DISTRIBUTED BY one column", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE dist_one(a int, b text) DISTRIBUTED BY (a)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE dist_one")
			oid := testutils.OidFromObjectName(connection, "public", "dist_one", backup.TYPE_RELATION)

			tables := []backup.Relation{{Oid: oid}}
			distPolicies := backup.GetDistributionPolicies(connection, tables)[oid]

			Expect(distPolicies).To(Equal("DISTRIBUTED BY (a)"))
		})
		It("returns distribution policy info for a table DISTRIBUTED BY two columns", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE dist_two(a int, b text) DISTRIBUTED BY (a, b)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE dist_two")
			oid := testutils.OidFromObjectName(connection, "public", "dist_two", backup.TYPE_RELATION)

			tables := []backup.Relation{{Oid: oid}}
			distPolicies := backup.GetDistributionPolicies(connection, tables)[oid]

			Expect(distPolicies).To(Equal("DISTRIBUTED BY (a, b)"))
		})
		It("returns distribution policy info for a table DISTRIBUTED BY column name as keyword", func() {
			testutils.AssertQueryRuns(connection, `CREATE TABLE dist_one(a int, "group" text) DISTRIBUTED BY ("group")`)
			defer testutils.AssertQueryRuns(connection, "DROP TABLE dist_one")
			oid := testutils.OidFromObjectName(connection, "public", "dist_one", backup.TYPE_RELATION)

			tables := []backup.Relation{{Oid: oid}}
			distPolicies := backup.GetDistributionPolicies(connection, tables)[oid]

			Expect(distPolicies).To(Equal(`DISTRIBUTED BY ("group")`))
		})
	})
	Describe("GetPartitionDefinitions", func() {
		It("returns empty string when no partition exists", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE simple_table(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE simple_table")
			oid := testutils.OidFromObjectName(connection, "public", "simple_table", backup.TYPE_RELATION)

			result := backup.GetPartitionDefinitions(connection)[oid]

			Expect(result).To(Equal(""))
		})
		It("returns a value for a partition definition", func() {
			testutils.AssertQueryRuns(connection, `CREATE TABLE part_table (id int, rank int, year int, gender 
char(1), count int ) 
DISTRIBUTED BY (id)
PARTITION BY LIST (gender)
( PARTITION girls VALUES ('F'), 
  PARTITION boys VALUES ('M'), 
  DEFAULT PARTITION other );
			`)
			defer testutils.AssertQueryRuns(connection, "DROP TABLE part_table")
			oid := testutils.OidFromObjectName(connection, "public", "part_table", backup.TYPE_RELATION)

			result := backup.GetPartitionDefinitions(connection)[oid]

			// The spacing is very specific here and is output from the postgres function
			expectedResult := `PARTITION BY LIST(gender) 
          (
          PARTITION girls VALUES('F') WITH (tablename='part_table_1_prt_girls', appendonly=false ), 
          PARTITION boys VALUES('M') WITH (tablename='part_table_1_prt_boys', appendonly=false ), 
          DEFAULT PARTITION other  WITH (tablename='part_table_1_prt_other', appendonly=false )
          )`
			Expect(result).To(Equal(expectedResult))
		})
	})
	Describe("GetPartitionTemplates", func() {
		It("returns empty string when no partition definition template exists", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE simple_table(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE simple_table")
			oid := testutils.OidFromObjectName(connection, "public", "simple_table", backup.TYPE_RELATION)

			result := backup.GetPartitionTemplates(connection)[oid]

			Expect(result).To(Equal(""))
		})
		It("returns a value for a subpartition template", func() {
			testutils.AssertQueryRuns(connection, `CREATE TABLE part_table (trans_id int, date date, amount decimal(9,2), region text)
  DISTRIBUTED BY (trans_id)
  PARTITION BY RANGE (date)
  SUBPARTITION BY LIST (region)
  SUBPARTITION TEMPLATE
    ( SUBPARTITION usa VALUES ('usa'),
      SUBPARTITION asia VALUES ('asia'),
      SUBPARTITION europe VALUES ('europe'),
      DEFAULT SUBPARTITION other_regions )
  ( START (date '2014-01-01') INCLUSIVE
    END (date '2014-04-01') EXCLUSIVE
    EVERY (INTERVAL '1 month') ) `)
			defer testutils.AssertQueryRuns(connection, "DROP TABLE part_table")
			oid := testutils.OidFromObjectName(connection, "public", "part_table", backup.TYPE_RELATION)

			result := backup.GetPartitionTemplates(connection)[oid]

			// The spacing is very specific here and is output from the postgres function
			expectedResult := `ALTER TABLE part_table 
SET SUBPARTITION TEMPLATE  
          (
          SUBPARTITION usa VALUES('usa') WITH (tablename='part_table'), 
          SUBPARTITION asia VALUES('asia') WITH (tablename='part_table'), 
          SUBPARTITION europe VALUES('europe') WITH (tablename='part_table'), 
          DEFAULT SUBPARTITION other_regions  WITH (tablename='part_table')
          )
`

			Expect(result).To(Equal(expectedResult))
		})
	})
	Describe("GetStorageOptions", func() {
		It("returns an empty string when no table storage options exist ", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE simple_table(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE simple_table")
			oid := testutils.OidFromObjectName(connection, "public", "simple_table", backup.TYPE_RELATION)

			result := backup.GetStorageOptions(connection)[oid]

			Expect(result).To(Equal(""))
		})
		It("returns a value for storage options of a table ", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE ao_table(i int) with (appendonly=true)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE ao_table")
			oid := testutils.OidFromObjectName(connection, "public", "ao_table", backup.TYPE_RELATION)

			result := backup.GetStorageOptions(connection)[oid]

			Expect(result).To(Equal("appendonly=true"))
		})
	})
	Describe("GetAllSequenceRelations", func() {
		It("returns a slice of all sequences", func() {
			testutils.AssertQueryRuns(connection, "CREATE SEQUENCE my_sequence START 10")
			defer testutils.AssertQueryRuns(connection, "DROP SEQUENCE my_sequence")
			testutils.AssertQueryRuns(connection, "COMMENT ON SEQUENCE public.my_sequence IS 'this is a sequence comment'")

			testutils.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
			defer testutils.AssertQueryRuns(connection, "DROP SCHEMA testschema CASCADE")
			testutils.AssertQueryRuns(connection, "CREATE SEQUENCE testschema.my_sequence2")

			sequences := backup.GetAllSequenceRelations(connection)

			mySequence := backup.BasicRelation("public", "my_sequence")
			mySequence2 := backup.BasicRelation("testschema", "my_sequence2")

			Expect(len(sequences)).To(Equal(2))
			testutils.ExpectStructsToMatchExcluding(&mySequence, &sequences[0], "SchemaOid", "Oid")
			testutils.ExpectStructsToMatchExcluding(&mySequence2, &sequences[1], "SchemaOid", "Oid")
		})
		It("returns a slice of all sequences in a specific schema", func() {
			testutils.AssertQueryRuns(connection, "CREATE SEQUENCE my_sequence START 10")
			defer testutils.AssertQueryRuns(connection, "DROP SEQUENCE my_sequence")

			testutils.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
			defer testutils.AssertQueryRuns(connection, "DROP SCHEMA testschema CASCADE")
			testutils.AssertQueryRuns(connection, "CREATE SEQUENCE testschema.my_sequence")
			mySequence := backup.BasicRelation("testschema", "my_sequence")

			backup.SetIncludeSchemas([]string{"testschema"})
			sequences := backup.GetAllSequenceRelations(connection)

			Expect(len(sequences)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&mySequence, &sequences[0], "SchemaOid", "Oid")
		})
	})
	Describe("GetSequenceDefinition", func() {
		It("returns sequence information for sequence with default values", func() {
			testutils.AssertQueryRuns(connection, "CREATE SEQUENCE my_sequence")
			defer testutils.AssertQueryRuns(connection, "DROP SEQUENCE my_sequence")

			resultSequenceDef := backup.GetSequenceDefinition(connection, "my_sequence")

			expectedSequence := backup.SequenceDefinition{Name: "my_sequence", LastVal: 1, Increment: 1, MaxVal: 9223372036854775807, MinVal: 1, CacheVal: 1}
			if connection.Version.Before("5") {
				expectedSequence.LogCnt = 1 // In GPDB 4.3, sequence log count is one-indexed
			}
			if connection.Version.AtLeast("6") {
				expectedSequence.StartVal = 1
			}

			testutils.ExpectStructsToMatch(&expectedSequence, &resultSequenceDef)
		})
		It("returns sequence information for a complex sequence", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE with_sequence(a int, b char(20))")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE with_sequence")
			testutils.AssertQueryRuns(connection,
				"CREATE SEQUENCE my_sequence INCREMENT BY 5 MINVALUE 20 MAXVALUE 1000 START 100 OWNED BY with_sequence.a")
			defer testutils.AssertQueryRuns(connection, "DROP SEQUENCE my_sequence")
			testutils.AssertQueryRuns(connection, "INSERT INTO with_sequence VALUES (nextval('my_sequence'), 'acme')")
			testutils.AssertQueryRuns(connection, "INSERT INTO with_sequence VALUES (nextval('my_sequence'), 'beta')")

			resultSequenceDef := backup.GetSequenceDefinition(connection, "my_sequence")

			expectedSequence := backup.SequenceDefinition{Name: "my_sequence", LastVal: 105, Increment: 5, MaxVal: 1000, MinVal: 20, CacheVal: 1, IsCycled: false, IsCalled: true}
			if connection.Version.Before("5") {
				expectedSequence.LogCnt = 32 // In GPDB 4.3, sequence log count is one-indexed
			} else {
				expectedSequence.LogCnt = 31 // In GPDB 5, sequence log count is zero-indexed
			}
			if connection.Version.AtLeast("6") {
				expectedSequence.StartVal = 100
			}

			testutils.ExpectStructsToMatch(&expectedSequence, &resultSequenceDef)
		})
	})
	Describe("GetSequenceOwnerMap", func() {
		It("returns sequence information for sequences owned by columns", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE without_sequence(a int, b char(20));")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE without_sequence")
			testutils.AssertQueryRuns(connection, "CREATE TABLE with_sequence(a int, b char(20));")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE with_sequence")
			testutils.AssertQueryRuns(connection, "CREATE SEQUENCE my_sequence OWNED BY with_sequence.a;")
			defer testutils.AssertQueryRuns(connection, "DROP SEQUENCE my_sequence")

			sequenceMap := backup.GetSequenceColumnOwnerMap(connection)

			Expect(len(sequenceMap)).To(Equal(1))
			Expect(sequenceMap["public.my_sequence"]).To(Equal("public.with_sequence.a"))
		})
	})
	Describe("GetAllSequences", func() {
		It("returns a slice of definitions for all sequences", func() {
			testutils.AssertQueryRuns(connection, "CREATE SEQUENCE seq_one START 3")
			defer testutils.AssertQueryRuns(connection, "DROP SEQUENCE seq_one")
			testutils.AssertQueryRuns(connection, "COMMENT ON SEQUENCE public.seq_one IS 'this is a sequence comment'")
			startValOne := int64(0)
			startValTwo := int64(0)
			if connection.Version.AtLeast("6") {
				startValOne = 3
				startValTwo = 7
			}

			testutils.AssertQueryRuns(connection, "CREATE SEQUENCE seq_two START 7")
			defer testutils.AssertQueryRuns(connection, "DROP SEQUENCE seq_two")

			seqOneRelation := backup.BasicRelation("public", "seq_one")
			seqOneDef := backup.SequenceDefinition{Name: "seq_one", LastVal: 3, Increment: 1, MaxVal: 9223372036854775807, MinVal: 1, CacheVal: 1, StartVal: startValOne}
			seqTwoRelation := backup.BasicRelation("public", "seq_two")
			seqTwoDef := backup.SequenceDefinition{Name: "seq_two", LastVal: 7, Increment: 1, MaxVal: 9223372036854775807, MinVal: 1, CacheVal: 1, StartVal: startValTwo}
			if connection.Version.Before("5") {
				seqOneDef.LogCnt = 1 // In GPDB 4.3, sequence log count is one-indexed
				seqTwoDef.LogCnt = 1
			}

			results := backup.GetAllSequences(connection)

			testutils.ExpectStructsToMatchExcluding(&seqOneRelation, &results[0].Relation, "SchemaOid", "Oid")
			testutils.ExpectStructsToMatchExcluding(&seqOneDef, &results[0].SequenceDefinition)
			testutils.ExpectStructsToMatchExcluding(&seqTwoRelation, &results[1].Relation, "SchemaOid", "Oid")
			testutils.ExpectStructsToMatchExcluding(&seqTwoDef, &results[1].SequenceDefinition)
		})
	})
	Describe("GetViews", func() {
		It("returns a slice for a basic view", func() {
			testutils.AssertQueryRuns(connection, "CREATE VIEW simpleview AS SELECT rolname FROM pg_roles")
			defer testutils.AssertQueryRuns(connection, "DROP VIEW simpleview")

			results := backup.GetViews(connection)

			viewDef := backup.View{Oid: 1, Schema: "public", Name: "simpleview", Definition: "SELECT pg_roles.rolname FROM pg_roles;", DependsUpon: nil}

			Expect(len(results)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&viewDef, &results[0], "Oid")
		})
		It("returns a slice for view in a specific schema", func() {
			testutils.AssertQueryRuns(connection, "CREATE VIEW simpleview AS SELECT rolname FROM pg_roles")
			defer testutils.AssertQueryRuns(connection, "DROP VIEW simpleview")
			testutils.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
			defer testutils.AssertQueryRuns(connection, "DROP SCHEMA testschema")
			testutils.AssertQueryRuns(connection, "CREATE VIEW testschema.simpleview AS SELECT rolname FROM pg_roles")
			defer testutils.AssertQueryRuns(connection, "DROP VIEW testschema.simpleview")
			backup.SetIncludeSchemas([]string{"testschema"})

			results := backup.GetViews(connection)

			viewDef := backup.View{Oid: 1, Schema: "testschema", Name: "simpleview", Definition: "SELECT pg_roles.rolname FROM pg_roles;", DependsUpon: nil}

			Expect(len(results)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&viewDef, &results[0], "Oid")
		})
	})
	Describe("ConstructTableDependencies", func() {
		child := backup.BasicRelation("public", "child")
		childOne := backup.BasicRelation("public", "child_one")
		childTwo := backup.BasicRelation("public", "child_two")
		tableDefs := map[uint32]backup.TableDefinition{}
		It("constructs dependencies correctly if there is one table dependent on one table", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE parent(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE parent")
			testutils.AssertQueryRuns(connection, "CREATE TABLE child() INHERITS (parent)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE child")

			child.Oid = testutils.OidFromObjectName(connection, "public", "child", backup.TYPE_RELATION)
			tables := []backup.Relation{child}

			tables = backup.ConstructTableDependencies(connection, tables, tableDefs, false)

			Expect(len(tables)).To(Equal(1))
			Expect(len(tables[0].DependsUpon)).To(Equal(1))
			Expect(tables[0].DependsUpon[0]).To(Equal("public.parent"))
			Expect(len(tables[0].Inherits)).To(Equal(1))
			Expect(tables[0].Inherits[0]).To(Equal("public.parent"))
		})
		It("constructs dependencies correctly if there are two tables dependent on one table", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE parent(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE parent")
			testutils.AssertQueryRuns(connection, "CREATE TABLE child_one() INHERITS (parent)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE child_one")
			testutils.AssertQueryRuns(connection, "CREATE TABLE child_two() INHERITS (parent)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE child_two")

			childOne.Oid = testutils.OidFromObjectName(connection, "public", "child_one", backup.TYPE_RELATION)
			childTwo.Oid = testutils.OidFromObjectName(connection, "public", "child_two", backup.TYPE_RELATION)
			tables := []backup.Relation{childOne, childTwo}

			tables = backup.ConstructTableDependencies(connection, tables, tableDefs, false)

			Expect(len(tables)).To(Equal(2))
			Expect(len(tables[0].DependsUpon)).To(Equal(1))
			Expect(tables[0].DependsUpon[0]).To(Equal("public.parent"))
			Expect(len(tables[0].Inherits)).To(Equal(1))
			Expect(tables[0].Inherits[0]).To(Equal("public.parent"))
			Expect(len(tables[1].DependsUpon)).To(Equal(1))
			Expect(tables[1].DependsUpon[0]).To(Equal("public.parent"))
			Expect(len(tables[1].Inherits)).To(Equal(1))
			Expect(tables[1].Inherits[0]).To(Equal("public.parent"))
		})
		It("constructs dependencies correctly if there is one table dependent on two tables", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE parent_one(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE parent_one")
			testutils.AssertQueryRuns(connection, "CREATE TABLE parent_two(j int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE parent_two")
			testutils.AssertQueryRuns(connection, "CREATE TABLE child() INHERITS (parent_one, parent_two)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE child")

			child.Oid = testutils.OidFromObjectName(connection, "public", "child", backup.TYPE_RELATION)
			tables := []backup.Relation{child}

			tables = backup.ConstructTableDependencies(connection, tables, tableDefs, false)

			sort.Strings(tables[0].DependsUpon)
			sort.Strings(tables[0].Inherits)
			Expect(len(tables)).To(Equal(1))
			Expect(len(tables[0].DependsUpon)).To(Equal(2))
			Expect(tables[0].DependsUpon[0]).To(Equal("public.parent_one"))
			Expect(tables[0].DependsUpon[1]).To(Equal("public.parent_two"))
			Expect(len(tables[0].Inherits)).To(Equal(2))
			Expect(tables[0].Inherits[0]).To(Equal("public.parent_one"))
			Expect(tables[0].Inherits[1]).To(Equal("public.parent_two"))
		})
		It("constructs dependencies correctly if there are no table dependencies", func() {
			tables := []backup.Relation{}
			tables = backup.ConstructTableDependencies(connection, tables, tableDefs, false)
			Expect(len(tables)).To(Equal(0))
		})
		It("constructs dependencies correctly if there are two dependent tables but one is not in the backup set", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE parent(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE parent")
			testutils.AssertQueryRuns(connection, "CREATE TABLE child_one() INHERITS (parent)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE child_one")
			testutils.AssertQueryRuns(connection, "CREATE TABLE child_two() INHERITS (parent)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE child_two")

			childOne.Oid = testutils.OidFromObjectName(connection, "public", "child_one", backup.TYPE_RELATION)
			tables := []backup.Relation{childOne}

			tables = backup.ConstructTableDependencies(connection, tables, tableDefs, true)

			Expect(len(tables)).To(Equal(1))
			Expect(len(tables[0].DependsUpon)).To(Equal(0))
			Expect(len(tables[0].Inherits)).To(Equal(1))
			Expect(tables[0].Inherits[0]).To(Equal("public.parent"))
		})
		It("does not record a dependency of an external leaf partition on a parent table", func() {
			testutils.AssertQueryRuns(connection, `CREATE TABLE partition_table (id int, gender char(1))
DISTRIBUTED BY (id)
PARTITION BY LIST (gender)
( PARTITION girls VALUES ('F'),
  PARTITION boys VALUES ('M'),
  DEFAULT PARTITION other );`)
			defer testutils.AssertQueryRuns(connection, "DROP TABLE partition_table")
			testutils.AssertQueryRuns(connection, `CREATE EXTERNAL WEB TABLE partition_table_ext_part_ (like partition_table_1_prt_girls)
EXECUTE 'echo -e "2\n1"' on host
FORMAT 'csv';`)
			defer testutils.AssertQueryRuns(connection, "DROP TABLE partition_table_ext_part_")
			testutils.AssertQueryRuns(connection, `ALTER TABLE public.partition_table EXCHANGE PARTITION girls WITH TABLE public.partition_table_ext_part_ WITHOUT VALIDATION;`)

			partition := backup.BasicRelation("public", "partition_table_ext_part_")
			partition.Oid = testutils.OidFromObjectName(connection, "public", "partition_table_ext_part_", backup.TYPE_RELATION)
			tables := []backup.Relation{partition}
			partTableDefs := map[uint32]backup.TableDefinition{partition.Oid: backup.TableDefinition{IsExternal: true, PartitionType: "l"}}

			tables = backup.ConstructTableDependencies(connection, tables, partTableDefs, false)

			Expect(len(tables)).To(Equal(1))
			Expect(len(tables[0].DependsUpon)).To(Equal(0))
			Expect(len(tables[0].Inherits)).To(Equal(0))
		})
	})
	Describe("ConstructViewDependencies", func() {
		It("constructs dependencies correctly for a view that depends on two other views", func() {
			testutils.AssertQueryRuns(connection, "CREATE VIEW parent1 AS SELECT relname FROM pg_class")
			defer testutils.AssertQueryRuns(connection, "DROP VIEW parent1")
			testutils.AssertQueryRuns(connection, "CREATE VIEW parent2 AS SELECT relname FROM pg_class")
			defer testutils.AssertQueryRuns(connection, "DROP VIEW parent2")
			testutils.AssertQueryRuns(connection, "CREATE VIEW child AS (SELECT * FROM parent1 UNION SELECT * FROM parent2)")
			defer testutils.AssertQueryRuns(connection, "DROP VIEW child")

			childView := backup.View{}
			childView.Oid = testutils.OidFromObjectName(connection, "public", "child", backup.TYPE_RELATION)
			views := []backup.View{childView}

			views = backup.ConstructViewDependencies(connection, views)

			Expect(len(views)).To(Equal(1))
			Expect(len(views[0].DependsUpon)).To(Equal(2))
			Expect(views[0].DependsUpon[0]).To(Equal("public.parent1"))
			Expect(views[0].DependsUpon[1]).To(Equal("public.parent2"))
		})
	})
})
