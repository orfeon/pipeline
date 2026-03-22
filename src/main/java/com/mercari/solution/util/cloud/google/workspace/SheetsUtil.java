package com.mercari.solution.util.cloud.google.workspace;

import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.drive.DriveScopes;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.model.*;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import com.google.cloud.hadoop.util.ChainingHttpRequestInitializer;
import com.google.common.collect.ImmutableList;
import com.mercari.solution.module.Schema;
import org.apache.beam.sdk.extensions.gcp.util.RetryHttpRequestInitializer;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.IOException;
import java.util.*;

public class SheetsUtil {

    private static final Map<String, Sheets> sheets = new HashMap<>();

    public enum Dimension {
        DIMENSION_UNSPECIFIED,
        ROWS,
        COLUMNS
    }

    public enum ValueRenderOption {
        FORMATTED_VALUE,
        UNFORMATTED_VALUE,
        FORMULA
    }

    public enum DateTimeRenderOption {
        SERIAL_NUMBER,
        FORMATTED_STRING
    }

    public static Sheets sheets(final String targetPrincipalAccount, String... args) {

        if(args.length == 0) {
            args = new String[]{DriveScopes.DRIVE_READONLY};
        }

        final HttpTransport transport = new NetHttpTransport();
        final JsonFactory jsonFactory = GsonFactory.getDefaultInstance();
        try {
            final GoogleCredentials credentials = GoogleCredentials.getApplicationDefault().createScoped(args);
            final ImpersonatedCredentials targetCredentials = ImpersonatedCredentials.create(
                    credentials,
                    targetPrincipalAccount,
                    null,
                    Arrays.asList(args),
                    3600);

            final HttpRequestInitializer initializer = new ChainingHttpRequestInitializer(
                    new HttpCredentialsAdapter(targetCredentials),
                    // Do not log 404. It clutters the output and is possibly even required by the caller.
                    new RetryHttpRequestInitializer(ImmutableList.of(404)));
            return new Sheets.Builder(transport, jsonFactory, initializer)
                    .setApplicationName("mercari-pipeline")
                    .build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Map<String, Object> get(
            final Sheets sheets,
            final String sheetId,
            final List<String> ranges) {

        try {
            final Spreadsheet spreadsheet = sheets.spreadsheets()
                    .get(sheetId)
                    .setRanges(ranges)
                    .setIncludeGridData(true)
                    .execute();

            return create(spreadsheet);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<List<Object>> values(
            final Sheets sheets,
            final String sheetId,
            final String range,
            final Dimension majorDimension,
            final ValueRenderOption valueRenderOption,
            final DateTimeRenderOption dateTimeRenderOption) {

        try {
            final Sheets.Spreadsheets.Values.BatchGet batchGet = sheets.spreadsheets()
                    .values()
                    .batchGet(sheetId)
                    .setRanges(List.of(range));
            if(majorDimension != null) {
                batchGet.setMajorDimension(majorDimension.name());
            }
            if(valueRenderOption != null) {
                batchGet.setValueRenderOption(valueRenderOption.name());
            }
            if(dateTimeRenderOption != null) {
                batchGet.setDateTimeRenderOption(dateTimeRenderOption.name());
            }

            final List<ValueRange> valueRanges = batchGet
                    .execute()
                    .getValueRanges();

            final List<List<Object>> results = new ArrayList<>();
            for(final ValueRange valueRange : valueRanges) {
                results.addAll(valueRange.getValues());
            }
            return results;

            /*
            final Sheets.Spreadsheets.Values.Get get = sheets.spreadsheets()
                    .values()
                    .get(sheetId, range);
            if(majorDimension != null) {
                get.setMajorDimension(majorDimension.name());
            }
            if(valueRenderOption != null) {
                get.setValueRenderOption(valueRenderOption.name());
            }
            if(dateTimeRenderOption != null) {
                get.setDateTimeRenderOption(dateTimeRenderOption.name());
            }
            final ValueRange response = get.execute();
            return response.getValues();
             */
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String toString(final CellData cellData) {
        if(cellData == null || cellData.getFormattedValue() == null) {
            return null;
        }
        final String formattedValue = cellData.getFormattedValue();
        final String hyperlink = cellData.getHyperlink();

        final List<String> formattedWithLinks = new ArrayList<>();
        if(hyperlink != null) {
            final String formattedWithLink = new StringBuilder(formattedValue)
                    .insert(0, "[").append("](")
                    .append(hyperlink)
                    .append(")")
                    .toString();
            formattedWithLinks.add(formattedWithLink);
        } else {
            final List<TextFormatRun> textFormatRuns = cellData.getTextFormatRuns();
            if(textFormatRuns != null) {
                int size = textFormatRuns.size();
                for(int i=0; i<size; i++) {
                    final TextFormatRun textFormatRun = textFormatRuns.get(i);
                    final TextFormat textFormat = textFormatRun.getFormat();
                    if(textFormat == null) {
                        continue;
                    }
                    final Link link = textFormat.getLink();
                    if(link == null || link.getUri() == null) {
                        continue;
                    }
                    final Integer startIndex = Optional.ofNullable(textFormatRun.getStartIndex()).orElse(0);
                    final Integer endIndex;
                    if(i+1 < size) {
                        endIndex = textFormatRuns.get(i+1).getStartIndex();
                    } else {
                        endIndex = formattedValue.length() - 1;
                    }

                    final String formattedWithLink = new StringBuilder(formattedValue.substring(startIndex, endIndex))
                            .insert(0, "[")
                            .append("](")
                            .append(link.getUri())
                            .append(")")
                            .toString();
                    formattedWithLinks.add(formattedWithLink);
                }
            }
            final List<ChipRun> chipRuns = cellData.getChipRuns();
            if(chipRuns != null) {
                int size = chipRuns.size();
                for(int i=0; i<size; i++) {
                    final ChipRun chipRun = chipRuns.get(i);
                    final Chip chip = chipRun.getChip();
                    if(chip == null) {
                        continue;
                    }
                    final RichLinkProperties richLinkProperties = chip.getRichLinkProperties();
                    if(richLinkProperties == null || richLinkProperties.getUri() == null) {
                        continue;
                    }
                    final Integer startIndex = Optional.ofNullable(chipRun.getStartIndex()).orElse(0);
                    final Integer endIndex;
                    if(i+1 < size) {
                        endIndex = chipRuns.get(i+1).getStartIndex();
                    } else {
                        endIndex = formattedValue.length() - 1;
                    }

                    final String formattedWithLink = new StringBuilder(formattedValue.substring(startIndex, endIndex))
                            .insert(0, "[")
                            .append("](")
                            .append(richLinkProperties.getUri())
                            .append(")")
                            .toString();
                    formattedWithLinks.add(formattedWithLink);
                }
            }
        }

        if(formattedWithLinks.isEmpty()) {
            formattedWithLinks.add(formattedValue);
        }

        String output = String.join(" ", formattedWithLinks);

        final String note = cellData.getNote();
        if(note != null) {
            final String formattedWithNote = "\n> [!NOTE]\n" +
                    "> " +
                    note;
            output = output + formattedWithNote;
        }

        return output;
    }

    private static String toCsvLine(List<?> values) {
        final StringBuilder sb = new StringBuilder();
        try(final CSVPrinter printer = new CSVPrinter(sb, CSVFormat.DEFAULT)) {
            printer.printRecord(values);
            printer.flush();
            return sb.toString().trim();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized static Sheets getOrCreateSheets(
            final String name,
            final String account) {

        return getOrCreateSheets(sheets, name, account, List.of(DriveScopes.DRIVE_READONLY));
    }

    public synchronized static Sheets getOrCreateSheets(
            final Map<String, Sheets> sheets,
            final String name,
            final String account) {

        return getOrCreateSheets(sheets, name, account, List.of(DriveScopes.DRIVE_READONLY));
    }

    public synchronized static Sheets getOrCreateSheets(
            final Map<String, Sheets> sheets,
            final String name,
            final String account,
            final List<String> scopes) {

        if(sheets.containsKey(name)) {
            final Sheets drive = sheets.get(name);
            if(drive != null) {
                return drive;
            }
        }
        createSheets(sheets, name, account, scopes);
        return sheets.get(name);
    }

    public synchronized static void createSheets(
            final Map<String, Sheets> sheets,
            final String name,
            final String account,
            final List<String> scopes) {

        if (sheets.containsKey(name)) {
            return;
        }

        final String[] args = scopes.toArray(new String[scopes.size()]);
        final Sheets drive = sheets(account, args);
        sheets.put(name, drive);
    }

    public static Map<String, Object> create(Spreadsheet spreadsheet) {
        final Map<String, Object> values = new HashMap<>();
        values.put("spreadsheetId", spreadsheet.getSpreadsheetId());
        values.put("spreadsheetUrl", spreadsheet.getSpreadsheetUrl());
        values.put("title", spreadsheet.getProperties().getTitle());
        values.put("locale", spreadsheet.getProperties().getLocale());
        values.put("timeZone", spreadsheet.getProperties().getTimeZone());
        final List<Map<String, Object>> sheets = new ArrayList<>();
        for(final Sheet sheet : spreadsheet.getSheets()) {
            final Map<String, Object> sheetValues = createSheet(sheet);
            sheets.add(sheetValues);
        }
        values.put("sheets", sheets);
        return values;
    }

    public static Map<String, Object> createSheet(Sheet sheet) {
        final Map<String, Object> values = new HashMap<>();
        values.put("sheetId", sheet.getProperties().getSheetId());
        values.put("title", sheet.getProperties().getTitle());
        values.put("index", sheet.getProperties().getIndex());
        values.put("sheetType", sheet.getProperties().getSheetType());
        values.put("hidden", sheet.getProperties().getHidden());
        values.put("rowCount", Optional.ofNullable(sheet.getProperties().getGridProperties()).map(GridProperties::getRowCount).orElse(null));
        values.put("columnCount", Optional.ofNullable(sheet.getProperties().getGridProperties()).map(GridProperties::getColumnCount).orElse(null));
        //values.put("frozenRowCount", Optional.ofNullable(sheet.getProperties().getGridProperties()).map(GridProperties::getFrozenRowCount).orElse(null));
        //values.put("frozenColumnCount", Optional.ofNullable(sheet.getProperties().getGridProperties()).map(GridProperties::getFrozenColumnCount).orElse(null));

        final String content = createSheetContent(sheet);
        values.put("content", content);

        return values;
    }

    public static String createSheetContent(Sheet sheet) {
        final List<GridData> gridDataList = sheet.getData();
        if(gridDataList == null) {
            return null;
        }
        List<String> lines = new ArrayList<>();
        for(final GridData gridData : gridDataList) {
            final List<RowData> rowDataList = gridData.getRowData();
            if(rowDataList == null) {
                continue;
            }
            for(final RowData rowData : rowDataList) {
                final List<CellData> cellDataList = rowData.getValues();
                if(cellDataList == null) {
                    continue;
                }
                final List<Object> list = new ArrayList<>();
                boolean flag = false;
                for(final CellData cellData : cellDataList) {
                    final String value = toString(cellData);
                    flag = flag || value != null;
                    list.add(value);
                }
                final String line = toCsvLine(list);
                if(flag) {
                    lines.add(line);
                }
            }
        }

        return String.join("\n", lines);
    }

    public static Schema createSchema() {
        return Schema.builder()
                .withField("spreadsheetId", Schema.FieldType.STRING)
                .withField("spreadsheetUrl", Schema.FieldType.STRING)
                .withField("title", Schema.FieldType.STRING)
                .withField("locale", Schema.FieldType.STRING)
                .withField("timeZone", Schema.FieldType.STRING)
                .withField("sheets", Schema.FieldType.array(Schema.FieldType.element(createSheetSchema())))
                .build();
    }

    public static Schema createSheetSchema() {
        return Schema.builder()
                .withField("title", Schema.FieldType.STRING)
                .withField("index", Schema.FieldType.INT32)
                .withField("sheetId", Schema.FieldType.INT32)
                .withField("sheetType", Schema.FieldType.STRING)
                .withField("hidden", Schema.FieldType.BOOLEAN)
                .withField("content", Schema.FieldType.STRING)
                .withField("rowCount", Schema.FieldType.INT32)
                .withField("columnCount", Schema.FieldType.INT32)
                //.withField("frozenRowCount", Schema.FieldType.INT32)
                //.withField("frozenColumnCount", Schema.FieldType.INT32)
                .build();
    }

}
