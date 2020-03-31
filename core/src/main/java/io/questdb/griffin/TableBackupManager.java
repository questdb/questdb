package io.questdb.griffin;

import io.questdb.cairo.AppendMemory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CairoSecurityContext;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.SymbolMapWriter;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.Path;

public class TableBackupManager {
	private final CairoConfiguration configuration;
	private final CairoEngine engine;
	private final SqlCompiler compiler;

	public TableBackupManager(CairoConfiguration configuration, CairoEngine engine, SqlCompiler compiler) {
		super();
		this.configuration = configuration;
		this.engine = engine;
		this.compiler = compiler;
	}

	public void backupTable(CairoSecurityContext securityContext, CharSequence tableName, Path path) {
		if (null == configuration.getBackupRoot()) {
			throw CairoException.instance(0).put("Backup is disabled, no backup root directory is configured in the server configuration ['cairo.sql.backup.root' property]");
		}

		try (TableReader reader = engine.getReader(securityContext, tableName)) {
			cloneMetaData(tableName, configuration.getFilesFacade(), path, configuration.getRoot(), configuration.getBackupRoot(), configuration.getMkDirMode());
			try (TableWriter backupWriter = engine.getBackupWriter(securityContext, tableName); RecordCursor cursor = reader.getCursor()) {
				compiler.copyTableData(cursor, reader.getMetadata(), backupWriter);
				backupWriter.commit();
			}
		}
	}

	private static void cloneMetaData(CharSequence tableName, FilesFacade ff, Path path, CharSequence root, CharSequence backupRoot, int mkDirMode) {
		path.of(root).concat(tableName).concat(TableUtils.META_FILE_NAME).$();
		try (TableReaderMetadata sourceMetaData = new TableReaderMetadata(ff, path)) {
			path.of(backupRoot).concat(tableName).put(Files.SEPARATOR).$();

			if (ff.exists(path)) {
				throw CairoException.instance(0).put("Backup dir for table \"" + tableName + "\" already exists [dir=").put(path).put(']');
			}

			if (ff.mkdirs(path, mkDirMode) != 0) {
				throw CairoException.instance(ff.errno()).put("Could not create [dir=").put(path).put(']');
			}

			final int rootLen = path.length();
			try (AppendMemory backupMem = new AppendMemory()) {
				backupMem.of(ff, path.trimTo(rootLen).concat(TableUtils.META_FILE_NAME).$(), ff.getPageSize());
				sourceMetaData.cloneTo(backupMem);

				// create symbol maps
				path.trimTo(rootLen).$();
				int symbolMapCount = 0;
				for (int i = 0; i < sourceMetaData.getColumnCount(); i++) {
					if (sourceMetaData.getColumnType(i) == ColumnType.SYMBOL) {
						SymbolMapWriter.createSymbolMapFiles(ff, backupMem, path, sourceMetaData.getColumnName(i), 128, true);
						symbolMapCount++;
					}
				}
				backupMem.of(ff, path.trimTo(rootLen).concat(TableUtils.TXN_FILE_NAME).$(), ff.getPageSize());
				TableUtils.resetTxn(backupMem, symbolMapCount, 0L, TableUtils.INITIAL_TXN);

			}
		}
	}

}
