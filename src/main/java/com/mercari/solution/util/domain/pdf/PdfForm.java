package com.mercari.solution.util.domain.pdf;

import org.apache.pdfbox.cos.COSName;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDResources;
import org.apache.pdfbox.pdmodel.font.PDFont;
import org.apache.pdfbox.pdmodel.interactive.form.*;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Fills the AcroForm fields of a document. */
public class PdfForm {

    private static final Pattern DA_FONT = Pattern.compile("/[^\\s/]+\\s+([-+]?[0-9.]+)\\s+Tf");
    private static final Set<String> TRUE_VALUES = Set.of("true", "yes", "on", "1", "y");

    private PdfForm() {}

    /** Fully qualified names of the terminal fields that take a value. */
    public static List<String> fieldNames(final PDDocument document) {
        final PDAcroForm form = document.getDocumentCatalog().getAcroForm();
        final List<String> names = new ArrayList<>();
        if(form == null) {
            return names;
        }
        for(final PDField field : form.getFieldTree()) {
            if(field instanceof PDTerminalField && !(field instanceof PDPushButton) && !(field instanceof PDSignatureField)) {
                names.add(field.getFullyQualifiedName());
            }
        }
        return names;
    }

    /**
     * Sets the values (fully qualified field name → value; a null value leaves the field as it is).
     *
     * @param font the font for text field values, or null to keep each field's own font
     * @param fonts the fonts of the document, used when {@code font} is set
     */
    public static void fill(
            final PDDocument document,
            final Map<String, Object> values,
            final String font,
            final PdfFonts.Resolver fonts,
            final boolean flatten) throws IOException {

        final PDAcroForm form = document.getDocumentCatalog().getAcroForm();
        if(form == null) {
            if(values.isEmpty()) {
                return;
            }
            throw new IllegalArgumentException("pdf has no form fields to set: " + values.keySet());
        }
        // a dynamic XFA form would be shown from its XFA data by viewers that support it, ignoring the AcroForm values
        if(form.hasXFA()) {
            form.setXFA(null);
        }
        COSName fontResource = null;
        if(font != null) {
            // an editable field keeps the whole font, so that a viewer can redraw a changed value
            final PDFont pdFont = fonts.get(font, flatten);
            PDResources resources = form.getDefaultResources();
            if(resources == null) {
                resources = new PDResources();
                form.setDefaultResources(resources);
            }
            fontResource = resources.add(pdFont);
        }
        for(final Map.Entry<String, Object> entry : values.entrySet()) {
            final PDField field = form.getField(entry.getKey());
            if(field == null) {
                throw new IllegalArgumentException("form field: " + entry.getKey() + " is not in the pdf, available: " + fieldNames(document));
            }
            if(entry.getValue() == null) {
                continue;
            }
            if(fontResource != null && field instanceof PDVariableText text) {
                text.setDefaultAppearance(withFont(text.getDefaultAppearance(), fontResource));
            }
            setValue(field, entry.getValue());
        }
        if(flatten) {
            final List<PDField> fields = new ArrayList<>();
            form.getFieldTree().forEach(fields::add);
            // with NeedAppearances the values were set without appearances, which flattening needs
            form.flatten(fields, form.getNeedAppearances());
        }
    }

    static String withFont(final String defaultAppearance, final COSName font) {
        final String da = defaultAppearance == null ? "" : defaultAppearance;
        final Matcher matcher = DA_FONT.matcher(da);
        if(matcher.find()) {
            return da.substring(0, matcher.start()) + "/" + font.getName() + " " + matcher.group(1) + " Tf" + da.substring(matcher.end());
        }
        return "/" + font.getName() + " 0 Tf " + (da.isBlank() ? "0 g" : da);
    }

    private static void setValue(final PDField field, final Object value) throws IOException {
        switch (field) {
            case PDCheckBox checkBox -> {
                if(isChecked(value, checkBox.getOnValue())) {
                    checkBox.check();
                } else {
                    checkBox.unCheck();
                }
            }
            case PDRadioButton radio -> radio.setValue(value.toString());
            case PDListBox list when value instanceof Collection<?> collection && list.isMultiSelect() ->
                    list.setValue(collection.stream().map(Object::toString).toList());
            case PDChoice choice -> choice.setValue(value.toString());
            case PDTextField text -> text.setValue(value.toString());
            case PDSignatureField ignored ->
                    throw new IllegalArgumentException("form field: " + field.getFullyQualifiedName() + " is a signature field and cannot be set");
            case PDPushButton ignored ->
                    throw new IllegalArgumentException("form field: " + field.getFullyQualifiedName() + " is a push button and cannot be set");
            default -> field.setValue(value.toString());
        }
    }

    private static boolean isChecked(final Object value, final String onValue) {
        if(value instanceof Boolean b) {
            return b;
        }
        final String text = value.toString().trim();
        return TRUE_VALUES.contains(text.toLowerCase()) || text.equals(onValue);
    }

}
