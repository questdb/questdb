package io.questdb.test.griffin.fuzz;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.arr.DirectArray;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.griffin.engine.join.HashJoinLightRecordCursorFactory;
import io.questdb.griffin.engine.join.HashOuterJoinLightRecordCursorFactory;
import io.questdb.griffin.engine.join.JoinRecordMetadata;
import io.questdb.griffin.model.JoinContext;
import io.questdb.griffin.model.QueryModel;
import io.questdb.std.Decimal256;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.cairo.TestRecord;
import io.questdb.test.griffin.fuzz.joins.AsofJoinOperatorFactory;
import io.questdb.test.griffin.fuzz.joins.CrossFullJoinOperatorFactory;
import io.questdb.test.griffin.fuzz.joins.CrossJoinOperatorFactory;
import io.questdb.test.griffin.fuzz.joins.CrossLeftJoinOperatorFactory;
import io.questdb.test.griffin.fuzz.joins.CrossRightJoinOperatorFactory;
import io.questdb.test.griffin.fuzz.joins.FullOuterJoinOperatorFactory;
import io.questdb.test.griffin.fuzz.joins.InnerJoinOperatorFactory;
import io.questdb.test.griffin.fuzz.joins.JoinOperatorFactory;
import io.questdb.test.griffin.fuzz.joins.LeftOuterJoinOperatorFactory;
import io.questdb.test.griffin.fuzz.joins.LtJoinOperatorFactory;
import io.questdb.test.griffin.fuzz.joins.RightOuterJoinOperatorFactory;
import io.questdb.test.griffin.fuzz.joins.SpliceJoinOperatorFactory;
import org.junit.Test;

public class OperatorFuzzTest extends AbstractCairoTest {
    private static final int ITERATIONS = 1000;
    private static final int MAX_JOINS = 3;
    private static final int MIN_TABLES = 2;
    private static final int MAX_TABLES = 5;
    private static final int MIN_COLUMNS = 6;
    private static final int MAX_COLUMNS = 14;

    /**
     * Exercises fuzz-generated operator pipelines on dynamically built all-types tables.
     */
    @Test
    public void testOperatorFuzz() throws Exception {
        assertMemoryLeak(() -> {
            Rnd rnd = new Rnd(0xaba, 0xaba);
            ObjList<TableSpec> tables = createRandomTables(rnd);
            for (int i = 0, n = tables.size(); i < n; i++) {
                createTable(tables.getQuick(i).model);
                insertRandomRows(rnd, tables.getQuick(i));
            }

            FuzzBuildContext context = new FuzzBuildContext(configuration);
            ObjList<JoinOperatorFactory> joinFactories = new ObjList<>();
            joinFactories.add(new CrossJoinOperatorFactory());
            joinFactories.add(new InnerJoinOperatorFactory());
            joinFactories.add(new LeftOuterJoinOperatorFactory());
            joinFactories.add(new RightOuterJoinOperatorFactory());
            joinFactories.add(new FullOuterJoinOperatorFactory());
            joinFactories.add(new CrossLeftJoinOperatorFactory());
            joinFactories.add(new CrossRightJoinOperatorFactory());
            joinFactories.add(new CrossFullJoinOperatorFactory());
            joinFactories.add(new AsofJoinOperatorFactory());
            joinFactories.add(new LtJoinOperatorFactory());
            joinFactories.add(new SpliceJoinOperatorFactory());
            // TODO: add a reference oracle for WINDOW joins and enable this again.

            FuzzQueryGenerator generator = new FuzzQueryGenerator(joinFactories, MAX_JOINS);

            runHashJoinLightCoverageCase(context, rnd);

            int hashJoinLightCount = 0;
            for (int i = 0; i < ITERATIONS; i++) {
                TableSpec leftTable = tables.getQuick(rnd.nextInt(tables.size()));
                TableSpec rightTable = tables.getQuick(rnd.nextInt(tables.size()));
                FuzzCase fuzzCase = generator.generate(
                        context,
                        rnd,
                        () -> selectNoProgress(leftTable.name),
                        () -> selectNoProgress(rightTable.name)
                );
                RecordCursorFactory baseFactory = ((SizeCheckingRecordCursorFactory) fuzzCase.underTest).getBaseFactory();
                if (baseFactory instanceof HashJoinLightRecordCursorFactory
                        || baseFactory instanceof HashOuterJoinLightRecordCursorFactory) {
                    hashJoinLightCount++;
                }
                System.out.println("iteration=" + i + "\n" + fuzzCase.description);
                try {
                    FuzzResultComparator.assertEqual(
                            context,
                            fuzzCase.underTest,
                            fuzzCase.oracle,
                            sqlExecutionContext,
                            fuzzCase.ordered
                    );
                } catch (AssertionError err) {
                    throw new AssertionError("iteration=" + i + ", " + fuzzCase.description, err);
                }
            }
            if (hashJoinLightCount == 0) {
                throw new AssertionError("hash join light factories were never exercised");
            }
        });
    }

    private ObjList<TableSpec> createRandomTables(Rnd rnd) {
        DomainPools pools = new DomainPools(rnd);
        int[] sharedDomainFrequency = createDomainFrequency(rnd, pools.size);
        int tableCount = MIN_TABLES + rnd.nextInt(MAX_TABLES - MIN_TABLES + 1);
        ObjList<TableSpec> tables = new ObjList<>();
        for (int i = 0; i < tableCount; i++) {
            tables.add(createTableSpec(rnd, pools, sharedDomainFrequency, i));
        }
        return tables;
    }

    private TableSpec createTableSpec(Rnd rnd, DomainPools pools, int[] sharedDomainFrequency, int index) {
        String name = "t" + index;
        boolean dense = rnd.nextBoolean();
        double baseNullChance = dense ? 0.05 : 0.6;
        int targetColumns = MIN_COLUMNS + rnd.nextInt(MAX_COLUMNS - MIN_COLUMNS + 1);
        int rows = dense ? 200 + rnd.nextInt(200) : 50 + rnd.nextInt(100);
        TableModel model = new TableModel(configuration, name, PartitionBy.DAY);
        boolean hasTimestamp = rnd.nextInt(20) != 0;
        int timestampType = ColumnType.UNDEFINED;
        if (hasTimestamp) {
            timestampType = rnd.nextBoolean() ? ColumnType.TIMESTAMP_MICRO : ColumnType.TIMESTAMP_NANO;
            model.timestamp("ts", timestampType);
        }
        int duplicatePercentage = dense ? 10 + rnd.nextInt(30) : 30 + rnd.nextInt(50);
        long timestampStart = randomTimestampStart(rnd, timestampType);
        long avgTimestampSpread = randomTimestampSpread(rnd, dense, timestampType);
        TableSpec spec = new TableSpec(
                name,
                model,
                rows,
                pools,
                duplicatePercentage,
                sharedDomainFrequency,
                timestampStart,
                avgTimestampSpread,
                timestampType
        );

        addColumn(spec, "id", ColumnType.LONG, 0.0, DomainKind.ID, true);
        if (rnd.nextBoolean()) {
            addColumn(spec, "sym", ColumnType.SYMBOL, baseNullChance, DomainKind.SYMBOL, rnd.nextInt(100) < 70);
        }

        int columnIndex = spec.columns.size();
        while (spec.columns.size() < targetColumns) {
            int type = randomColumnType(rnd);
            String colName = "c" + columnIndex++;
            if (type == ColumnType.SYMBOL) {
                addColumn(spec, colName, type, jitterNullChance(rnd, baseNullChance), DomainKind.SYMBOL, rnd.nextInt(100) < 70);
            } else if (type == ColumnType.STRING) {
                addColumn(spec, colName, type, jitterNullChance(rnd, baseNullChance), DomainKind.STRING, rnd.nextInt(100) < 70);
            } else if (type == ColumnType.VARCHAR) {
                addColumn(spec, colName, type, jitterNullChance(rnd, baseNullChance), DomainKind.VARCHAR, rnd.nextInt(100) < 70);
            } else if (type == ColumnType.INT) {
                addColumn(spec, colName, type, jitterNullChance(rnd, baseNullChance), DomainKind.INT, rnd.nextInt(100) < 70);
            } else if (type == ColumnType.LONG) {
                addColumn(spec, colName, type, jitterNullChance(rnd, baseNullChance), DomainKind.LONG, rnd.nextInt(100) < 70);
            } else {
                addColumn(spec, colName, type, jitterNullChance(rnd, baseNullChance), DomainKind.NONE, false);
            }
        }

        for (int i = 0, n = spec.columns.size(); i < n; i++) {
            ColumnSpec column = spec.columns.getQuick(i);
            model.col(column.name, column.type);
            if (ColumnType.tagOf(column.type) == ColumnType.SYMBOL) {
                model.symbolCapacity(64);
            }
            column.index = hasTimestamp ? i + 1 : i;
        }
        return spec;
    }

    private int[] createDomainFrequency(Rnd rnd, int size) {
        int[] cumulative = new int[size];
        int sum = 0;
        for (int i = 0; i < size; i++) {
            int c = rnd.nextPositiveInt() % 100;
            int frequency = switch (c <= 2 ? 0 : c <= 4 ? 1 : 2) {
                // Very rare
                case 0 -> rnd.nextPositiveInt() % 5;
                // A lot
                case 1 -> 100 + rnd.nextPositiveInt() % 5000;
                // Average
                default -> 50 + rnd.nextPositiveInt() % 100;
            };
            sum += frequency;
            cumulative[i] = sum;
        }
        if (sum == 0) {
            for (int i = 0; i < size; i++) {
                cumulative[i] = i + 1;
            }
        }
        return cumulative;
    }

    private long randomTimestampStart(Rnd rnd, int timestampType) {
        long base = timestampType == ColumnType.TIMESTAMP_NANO
                ? 1_600_000_000_000_000_000L
                : 1_600_000_000_000L;
        long jitter = timestampType == ColumnType.TIMESTAMP_NANO ? 1_000_000_000L : 1_000_000L;
        return base + (rnd.nextPositiveLong() % jitter);
    }

    private long randomTimestampSpread(Rnd rnd, boolean dense, int timestampType) {
        long base = dense ? 1_000 : 50_000;
        if (timestampType == ColumnType.TIMESTAMP_NANO) {
            base *= 1_000;
        }
        return base + (rnd.nextPositiveLong() % (base * 10));
    }

    private int pickWeightedIndex(int[] cumulative, Rnd rnd) {
        if (cumulative.length == 0) {
            return 0;
        }
        int total = cumulative[cumulative.length - 1];
        if (total <= 0) {
            return rnd.nextInt(cumulative.length);
        }
        int pick = rnd.nextInt(total);
        for (int i = 0; i < cumulative.length; i++) {
            if (pick < cumulative[i]) {
                return i;
            }
        }
        return cumulative.length - 1;
    }

    private void runHashJoinLightCoverageCase(FuzzBuildContext context, Rnd rnd) throws Exception {
        HashJoinLightTables tables = createHashJoinLightCoverageTables(rnd);
        RecordCursorFactory leftUnderTest = selectNoProgress(tables.leftName);
        RecordCursorFactory rightUnderTest = selectNoProgress(tables.rightName);
        RecordCursorFactory leftOracle = selectNoProgress(tables.leftName);
        RecordCursorFactory rightOracle = selectNoProgress(tables.rightName);
        RecordAtCountingCursorFactory countingRight = new RecordAtCountingCursorFactory(rightUnderTest);
        try {
            if (!leftUnderTest.recordCursorSupportsRandomAccess() || !rightUnderTest.recordCursorSupportsRandomAccess()) {
                return;
            }

            RecordMetadata leftMetadata = leftUnderTest.getMetadata();
            RecordMetadata rightMetadata = rightUnderTest.getMetadata();
            int leftId = leftMetadata.getColumnIndex("id");
            int rightId = rightMetadata.getColumnIndex("id");
            if (leftId == -1 || rightId == -1) {
                throw new AssertionError("hash join light coverage tables must include id");
            }
            IntList leftKeys = new IntList();
            IntList rightKeys = new IntList();
            leftKeys.add(leftId);
            rightKeys.add(rightId);
            JoinKeyPlan keyPlan = context.planJoinKeys(
                    leftMetadata,
                    rightMetadata,
                    leftKeys,
                    rightKeys,
                    false
            );
            JoinContext joinContext = new JoinContext();
            JoinRecordMetadata joinMetadata = context.createJoinMetadata(
                    leftMetadata,
                    rightMetadata,
                    leftMetadata.getTimestampIndex(),
                    "slave0"
            );
            ArrayColumnTypes valueTypes = new ArrayColumnTypes();
            valueTypes.add(ColumnType.INT);
            valueTypes.add(ColumnType.INT);
            RecordCursorFactory underTest = new HashJoinLightRecordCursorFactory(
                    configuration,
                    joinMetadata,
                    leftUnderTest,
                    countingRight,
                    keyPlan.keyTypes,
                    valueTypes,
                    keyPlan.masterKeySink,
                    keyPlan.slaveKeySink,
                    leftMetadata.getColumnCount(),
                    joinContext
            );
            RecordCursorFactory oracle = new ReferenceJoinRecordCursorFactory(
                    leftOracle,
                    rightOracle,
                    joinMetadata,
                    QueryModel.JOIN_INNER,
                    keyPlan,
                    null
            );
            FuzzResultComparator.assertEqual(
                    context,
                    new SizeCheckingRecordCursorFactory(underTest),
                    new SizeCheckingRecordCursorFactory(oracle),
                    sqlExecutionContext,
                    false
            );
            if (countingRight.getRecordAtCount() == 0) {
                throw new AssertionError("HashJoinLightRecordCursorFactory did not call slave recordAt");
            }
        } finally {
            countingRight.close();
            leftUnderTest.close();
            leftOracle.close();
            rightOracle.close();
        }
    }

    private int randomColumnType(Rnd rnd) {
        int pick = rnd.nextInt(22);
        switch (pick) {
            case 0:
                return ColumnType.BOOLEAN;
            case 1:
                return ColumnType.BYTE;
            case 2:
                return ColumnType.SHORT;
            case 3:
                return ColumnType.CHAR;
            case 4:
                return ColumnType.INT;
            case 5:
                return ColumnType.LONG;
            case 6:
                return ColumnType.DATE;
            case 7:
                return ColumnType.TIMESTAMP_NANO;
            case 8:
                return ColumnType.FLOAT;
            case 9:
                return ColumnType.DOUBLE;
            case 10:
                return ColumnType.STRING;
            case 11:
                return ColumnType.SYMBOL;
            case 12:
                return ColumnType.VARCHAR;
            case 13:
                return ColumnType.BINARY;
            case 14:
                return ColumnType.LONG128;
            case 15:
                return ColumnType.LONG256;
            case 16:
                return ColumnType.UUID;
            case 17:
                return ColumnType.IPv4;
            case 18:
                return ColumnType.getGeoHashTypeWithBits(5);
            case 19:
                return ColumnType.getGeoHashTypeWithBits(20);
            case 20:
                return ColumnType.encodeArrayType(ColumnType.DOUBLE, 1);
            default:
                return ColumnType.getDecimalType(18, 4);
        }
    }

    private double jitterNullChance(Rnd rnd, double base) {
        double jitter = (rnd.nextDouble() - 0.5) * 0.2;
        double value = base + jitter;
        if (value < 0.0) {
            return 0.0;
        }
        if (value > 0.9) {
            return 0.9;
        }
        return value;
    }

    private void addColumn(TableSpec spec, String name, int type, double nullChance, DomainKind domain, boolean correlated) {
        spec.columns.add(new ColumnSpec(name, type, nullChance, domain, correlated));
    }

    private void insertRandomRows(Rnd rnd, TableSpec spec) {
        try (TableWriter writer = getWriter(spec.name);
             DirectArray array = new DirectArray()) {
            // Write rows via TableWriter to avoid SQL string generation and keep all types covered.
            RecordMetadata metadata = writer.getMetadata();
            int timestampIndex = metadata.getTimestampIndex();
            Utf8StringSink utf8Sink = new Utf8StringSink();
            TestRecord.ArrayBinarySequence binarySequence = new TestRecord.ArrayBinarySequence();
            Decimal256 decimal = new Decimal256();

            long ts = spec.timestampStart;
            long spread = Math.max(1, spec.avgTimestampSpread);
            int domainIndex = 0;
            for (int i = 0; i < spec.rows; i++) {
                // Keep stretches of repeated timestamp/domain values to mimic skewed time series with duplicates.
                boolean newKey = i == 0 || rnd.nextInt(100) < 100 - spec.duplicatePercentage;
                if (newKey) {
                    ts += 1 + (rnd.nextPositiveLong() % (spread * 2));
                    domainIndex = pickWeightedIndex(spec.domainFrequency, rnd);
                }
                long rowTimestamp = ts;
                TableWriter.Row row = timestampIndex == -1 ? writer.newRow() : writer.newRow(rowTimestamp);
                for (int colIndex = 0, n = spec.columns.size(); colIndex < n; colIndex++) {
                    ColumnSpec column = spec.columns.getQuick(colIndex);
                    int columnIndex = column.index;
                    if (columnIndex == timestampIndex) {
                        continue;
                    }
                    boolean isNull = column.nullChance > 0 && rnd.nextDouble() < column.nullChance;
                    if (column.domain != DomainKind.NONE) {
                        int valueIndex = column.correlated
                                ? domainIndex
                                : pickWeightedIndex(spec.domainFrequency, rnd);
                        writeDomainValue(row, column, isNull, utf8Sink, spec.domains, valueIndex);
                    } else {
                        appendColumnValue(
                                rnd,
                                column.type,
                                row,
                                columnIndex,
                                isNull,
                                rowTimestamp,
                                spread,
                                utf8Sink,
                                binarySequence,
                                array,
                                decimal,
                                spec.domains.symbols
                        );
                    }
                }
                row.append();
            }
        }
    }

    private void writeDomainValue(
            TableWriter.Row row,
            ColumnSpec column,
            boolean isNull,
            Utf8StringSink utf8Sink,
            DomainPools domains,
            int valueIndex
    ) {
        int index = column.index;
        switch (column.domain) {
            case ID: {
                long value = domains.idValues[valueIndex];
                row.putLong(index, value);
                break;
            }
            case INT: {
                if (isNull) {
                    row.putInt(index, Numbers.INT_NULL);
                    break;
                }
                int value = domains.intValues[valueIndex];
                row.putInt(index, value);
                break;
            }
            case LONG: {
                if (isNull) {
                    row.putLong(index, Numbers.LONG_NULL);
                    break;
                }
                long value = domains.longValues[valueIndex];
                row.putLong(index, value);
                break;
            }
            case SYMBOL: {
                row.putSym(index, isNull ? null : domains.symbols[valueIndex]);
                break;
            }
            case STRING: {
                row.putStr(index, isNull ? null : domains.strings[valueIndex]);
                break;
            }
            case VARCHAR: {
                if (isNull) {
                    row.putVarchar(index, null);
                    break;
                }
                utf8Sink.clear();
                utf8Sink.put(domains.varchars[valueIndex]);
                row.putVarchar(index, utf8Sink);
                break;
            }
            default:
                break;
        }
    }

    private void appendColumnValue(
            Rnd rnd,
            int type,
            TableWriter.Row row,
            int columnIndex,
            boolean isNull,
            long rowTimestamp,
            long timestampJitter,
            Utf8StringSink utf8Sink,
            TestRecord.ArrayBinarySequence binarySequence,
            DirectArray array,
            Decimal256 decimal,
            String[] symbols
    ) {
        // Centralized value generator so fuzz tables include every persisted type consistently.
        switch (ColumnType.tagOf(type)) {
            case ColumnType.CHAR:
                row.putChar(columnIndex, rnd.nextChar());
                break;
            case ColumnType.INT:
                row.putInt(columnIndex, isNull ? Numbers.INT_NULL : rnd.nextInt());
                break;
            case ColumnType.IPv4:
                row.putIPv4(columnIndex, isNull ? Numbers.IPv4_NULL : rnd.nextInt());
                break;
            case ColumnType.LONG:
                row.putLong(columnIndex, isNull ? Numbers.LONG_NULL : rnd.nextLong());
                break;
            case ColumnType.TIMESTAMP:
                row.putTimestamp(columnIndex, isNull ? Numbers.LONG_NULL : rowTimestamp + rnd.nextLong(timestampJitter));
                break;
            case ColumnType.DATE:
                row.putDate(columnIndex, isNull ? Numbers.LONG_NULL : rowTimestamp + rnd.nextLong(timestampJitter));
                break;
            case ColumnType.SYMBOL:
                row.putSym(columnIndex, isNull ? null : symbols[rnd.nextInt(symbols.length)]);
                break;
            case ColumnType.FLOAT:
                row.putFloat(columnIndex, isNull ? Float.NaN : rnd.nextFloat());
                break;
            case ColumnType.SHORT:
                row.putShort(columnIndex, isNull ? 0 : rnd.nextShort());
                break;
            case ColumnType.BYTE:
                row.putByte(columnIndex, isNull ? 0 : rnd.nextByte());
                break;
            case ColumnType.BOOLEAN:
                row.putBool(columnIndex, rnd.nextBoolean());
                break;
            case ColumnType.LONG128:
                row.putLong128(
                        columnIndex,
                        isNull ? Numbers.LONG_NULL : rnd.nextLong(),
                        isNull ? Numbers.LONG_NULL : rnd.nextLong()
                );
                break;
            case ColumnType.LONG256:
                row.putLong256(
                        columnIndex,
                        isNull ? Numbers.LONG_NULL : rnd.nextLong(),
                        isNull ? Numbers.LONG_NULL : rnd.nextLong(),
                        isNull ? Numbers.LONG_NULL : rnd.nextLong(),
                        isNull ? Numbers.LONG_NULL : rnd.nextLong()
                );
                break;
            case ColumnType.DOUBLE:
                row.putDouble(columnIndex, isNull ? Double.NaN : rnd.nextDouble());
                break;
            case ColumnType.VARCHAR:
                if (isNull) {
                    row.putVarchar(columnIndex, null);
                    break;
                }
                utf8Sink.clear();
                int varcharLen = rnd.nextPositiveInt() % 16;
                rnd.nextUtf8Str(varcharLen, utf8Sink);
                row.putVarchar(columnIndex, utf8Sink);
                break;
            case ColumnType.STRING:
                row.putStr(columnIndex, isNull ? null : rnd.nextString(rnd.nextPositiveInt() % 16));
                break;
            case ColumnType.BINARY:
                int len = rnd.nextPositiveInt() % 16;
                row.putBin(columnIndex, isNull ? null : binarySequence.of(len == 0 ? new byte[0] : rnd.nextBytes(len)));
                break;
            case ColumnType.GEOBYTE:
            case ColumnType.GEOSHORT:
            case ColumnType.GEOINT:
            case ColumnType.GEOLONG:
                row.putGeoHash(columnIndex, rnd.nextLong());
                break;
            case ColumnType.UUID:
                row.putLong128(
                        columnIndex,
                        isNull ? Numbers.LONG_NULL : rnd.nextLong(),
                        isNull ? Numbers.LONG_NULL : rnd.nextLong()
                );
                break;
            case ColumnType.ARRAY:
                if (isNull) {
                    array.ofNull();
                } else {
                    rnd.nextDoubleArray(ColumnType.decodeArrayDimensionality(type), array, 1, 8, 0);
                }
                row.putArray(columnIndex, array);
                break;
            case ColumnType.DECIMAL8:
            case ColumnType.DECIMAL16:
            case ColumnType.DECIMAL32:
            case ColumnType.DECIMAL64:
            case ColumnType.DECIMAL128:
            case ColumnType.DECIMAL256: {
                if (isNull) {
                    decimal.ofNull();
                } else {
                    int precision = ColumnType.getDecimalPrecision(type);
                    int scale = ColumnType.getDecimalScale(type);
                    int maxDigits = Math.min(precision, Numbers.pow10max);
                    long limit = Numbers.pow10[maxDigits] - 1;
                    long value = rnd.nextLong(limit + 1);
                    if (rnd.nextBoolean()) {
                        value = -value;
                    }
                    decimal.ofLong(value, scale);
                }
                row.putDecimal(columnIndex, decimal);
                break;
            }
            default:
                throw new UnsupportedOperationException("Unsupported type: " + ColumnType.nameOf(type));
        }
    }

    private static final class TableSpec {
        final String name;
        final TableModel model;
        final ObjList<ColumnSpec> columns = new ObjList<>();
        final int rows;
        final DomainPools domains;
        final int duplicatePercentage;
        final int[] domainFrequency;
        final long timestampStart;
        final long avgTimestampSpread;
        final int timestampType;

        TableSpec(
                String name,
                TableModel model,
                int rows,
                DomainPools domains,
                int duplicatePercentage,
                int[] domainFrequency,
                long timestampStart,
                long avgTimestampSpread,
                int timestampType
        ) {
            this.name = name;
            this.model = model;
            this.rows = rows;
            this.domains = domains;
            this.duplicatePercentage = duplicatePercentage;
            this.domainFrequency = domainFrequency;
            this.timestampStart = timestampStart;
            this.avgTimestampSpread = avgTimestampSpread;
            this.timestampType = timestampType;
        }
    }

    private static final class ColumnSpec {
        final String name;
        final int type;
        final double nullChance;
        final DomainKind domain;
        final boolean correlated;
        int index;

        ColumnSpec(String name, int type, double nullChance, DomainKind domain, boolean correlated) {
            this.name = name;
            this.type = type;
            this.nullChance = nullChance;
            this.domain = domain;
            this.correlated = correlated;
        }
    }

    private enum DomainKind {
        NONE,
        ID,
        INT,
        LONG,
        SYMBOL,
        STRING,
        VARCHAR
    }

    private static final class DomainPools {
        final int size;
        final long[] idValues;
        final int[] intValues;
        final long[] longValues;
        final String[] symbols;
        final String[] strings;
        final String[] varchars;

        DomainPools(Rnd rnd) {
            size = 16 + rnd.nextInt(16);
            idValues = new long[size];
            intValues = new int[size];
            longValues = new long[size];
            symbols = new String[size];
            strings = new String[size];
            varchars = new String[size];
            for (int i = 0; i < size; i++) {
                idValues[i] = rnd.nextPositiveLong() % 64;
                intValues[i] = rnd.nextInt(128);
                longValues[i] = rnd.nextPositiveLong() % 512;
                symbols[i] = "sym" + i;
                strings[i] = "str" + i;
                varchars[i] = "vch" + i;
            }
        }
    }

    private HashJoinLightTables createHashJoinLightCoverageTables(Rnd rnd) {
        String leftName = "hj_left";
        String rightName = "hj_right";
        TableModel leftModel = new TableModel(configuration, leftName, PartitionBy.DAY)
                .timestamp("ts", ColumnType.TIMESTAMP_MICRO)
                .col("id", ColumnType.LONG)
                .col("sym", ColumnType.SYMBOL);
        TableModel rightModel = new TableModel(configuration, rightName, PartitionBy.DAY)
                .timestamp("ts", ColumnType.TIMESTAMP_MICRO)
                .col("id", ColumnType.LONG)
                .col("sym", ColumnType.SYMBOL);
        createTable(leftModel);
        createTable(rightModel);

        long ts = 1_600_000_000_000L + (rnd.nextPositiveLong() % 1_000L);
        try (TableWriter w = getWriter(leftName)) {
            for (int i = 0; i < 20; i++) {
                TableWriter.Row row = w.newRow(ts + i);
                row.putLong(1, i % 6);
                row.putSym(2, "sym" + (i % 3));
                row.append();
            }
            w.commit();
        }

        try (TableWriter w = getWriter(rightName)) {
            for (int i = 0; i < 12; i++) {
                TableWriter.Row row = w.newRow(ts + i);
                row.putLong(1, i % 6);
                row.putSym(2, "sym" + (i % 3));
                row.append();
            }
            w.commit();
        }
        return new HashJoinLightTables(leftName, rightName);
    }

    private static final class HashJoinLightTables {
        final String leftName;
        final String rightName;

        HashJoinLightTables(String leftName, String rightName) {
            this.leftName = leftName;
            this.rightName = rightName;
        }
    }

    private static final class RecordAtCountingCursorFactory extends io.questdb.cairo.AbstractRecordCursorFactory {
        private final RecordCursorFactory base;
        private final CountingCursor cursor = new CountingCursor();
        private long recordAtCount;

        RecordAtCountingCursorFactory(RecordCursorFactory base) {
            super(base.getMetadata());
            this.base = base;
        }

        long getRecordAtCount() {
            return recordAtCount;
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
            RecordCursor baseCursor = base.getCursor(executionContext);
            cursor.of(baseCursor);
            return cursor;
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return base.recordCursorSupportsRandomAccess();
        }

        @Override
        public int getScanDirection() {
            return base.getScanDirection();
        }

        @Override
        public boolean supportsUpdateRowId(io.questdb.cairo.TableToken tableToken) {
            return base.supportsUpdateRowId(tableToken);
        }

        @Override
        public void toPlan(PlanSink sink) {
            base.toPlan(sink);
        }

        @Override
        protected void _close() {
            base.close();
        }

        private final class CountingCursor implements RecordCursor {
            private RecordCursor baseCursor;

            void of(RecordCursor baseCursor) {
                this.baseCursor = baseCursor;
            }

            @Override
            public Record getRecord() {
                return baseCursor.getRecord();
            }

            @Override
            public Record getRecordB() {
                return baseCursor.getRecordB();
            }

            @Override
            public boolean hasNext() {
                return baseCursor.hasNext();
            }

            @Override
            public void recordAt(Record record, long atRowId) {
                recordAtCount++;
                baseCursor.recordAt(record, atRowId);
            }

            @Override
            public void toTop() {
                baseCursor.toTop();
            }

            @Override
            public long size() {
                return baseCursor.size();
            }

            @Override
            public long preComputedStateSize() {
                return baseCursor.preComputedStateSize();
            }

            @Override
            public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
                baseCursor.calculateSize(circuitBreaker, counter);
            }

            @Override
            public SymbolTable getSymbolTable(int columnIndex) {
                return baseCursor.getSymbolTable(columnIndex);
            }

            @Override
            public SymbolTable newSymbolTable(int columnIndex) {
                return baseCursor.newSymbolTable(columnIndex);
            }

            @Override
            public void close() {
                baseCursor.close();
            }
        }
    }

    private static RecordCursorFactory selectNoProgress(CharSequence sql) throws SqlException {
        RecordCursorFactory factory = select(sql);
        if (factory instanceof QueryProgress progress) {
            return progress.getBaseFactory();
        }
        return factory;
    }
}
