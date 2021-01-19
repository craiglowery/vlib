package com.craiglowery.java.common;


import com.craiglowery.java.vlib.clients.core.NameValuePair;
import com.craiglowery.java.vlib.clients.core.NameValuePairList;
import javafx.scene.control.Alert;
import javafx.scene.control.ButtonBar;
import javafx.scene.control.ButtonType;
import javafx.scene.control.TextInputDialog;
import org.w3c.dom.Document;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.*;
import java.nio.file.Path;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static java.time.LocalDate.of;

/**
 * Created by Craig on 4/16/2016.
 */
public class Util {

    private final static String VIDEO_EXTENSIONS_REGEX = "\\.(avi|mpg|ts|m2ts|mp4|mkv)$";
    private static Pattern videoExtensionsPattern = null;

    static {
        try {
            videoExtensionsPattern = Pattern.compile(VIDEO_EXTENSIONS_REGEX);
        } catch (PatternSyntaxException e) {
            System.err.println("VIDEO_EXTENSIONS_REGEX pattern does not compile");
        }
    }

    public static String endsWithKnownVideoExtension(String filename)
    {
        try {
            Matcher m = videoExtensionsPattern.matcher(filename);
            if (m.find()) {
                return m.group(0);
            }
        } catch (Exception e) { /* ignore */ }
        return null;
    }


    /**
     * User choices (buttons) for showMessage dialogs.
     */
    public enum MessageBoxChoices {OK, YES, NO, CANCEL};

    /**
     * Presents a configurable message dialog box.
     * @param header  The headline message, such as "An error occurred while retrieving data."
     * @param message The question or detail message, such as "Would you like to retry?" or "Try again later."
     * @param type The AlertType, WARNING, CONFIRMATION, ERROR, INFORMATION, NONE
     * @param choices One more more choices OK, YES, NO and CANCEL.  The default is OK if none are specified.
     * @return The choice the user made.  If the user closes the dialog without making a choice, CANCEL is returned.
     */
    public static MessageBoxChoices showMessage(String header, String message, Alert.AlertType type, MessageBoxChoices... choices) {
        if (choices == null || choices.length==0)
            choices = new MessageBoxChoices[]{ MessageBoxChoices.OK };
        Map<ButtonType,MessageBoxChoices> resultMap = new HashMap<>();

        Alert dialog = new Alert(type);
        dialog.setHeaderText(header);
        dialog.setContentText(message);
        ButtonType[] buttons = new ButtonType[choices.length];
        for (int x = 0; x<choices.length; x++) {
            switch (choices[x]) {
                case CANCEL:
                    buttons[x] = new ButtonType("Cancel", ButtonBar.ButtonData.CANCEL_CLOSE);
                    break;
                case NO:
                    buttons[x] = new ButtonType("No", ButtonBar.ButtonData.NO);
                    break;
                case YES:
                    buttons[x] = new ButtonType("Yes", ButtonBar.ButtonData.YES);
                    break;
                case OK:
                    buttons[x] = new ButtonType("OK",ButtonBar.ButtonData.OK_DONE);
                    break;
            }
            resultMap.put(buttons[x],choices[x]);
        }
        dialog.getButtonTypes().setAll(buttons);
        Optional<ButtonType> result = dialog.showAndWait();
        if (result.isPresent())
            return resultMap.get(result.get());
        return MessageBoxChoices.CANCEL;
    }

    /**
     * Shows a simple text input box.
     * @param title The title of the dialog.
     * @param header The header of the dialog.
     * @param message The message prompt.
     * @return The string entered, or null if the dialog was canceled.
     */
    public static String showTextInputDialog(String title, String header, String message) {
        TextInputDialog tid = new TextInputDialog();
        tid.setContentText(message);
        tid.setHeaderText(header);
        tid.setTitle(title);
        tid.setWidth(500);
        Optional<String> result = tid.showAndWait();
        if (result.isPresent())
            return result.get();
        return null;
    }

    /**
     * Shows a simple error message with an OK button.
     * @param header
     * @param message
     */
    public static void showError(String header,String message) {
        showMessage(header,message, Alert.AlertType.ERROR,MessageBoxChoices.OK);
    }

    /**
     * Shows a simple information message with an OK button.
     * @param header
     * @param message
     */
    public static void showInformation(String header,String message) {
        showMessage(header,message, Alert.AlertType.INFORMATION,MessageBoxChoices.OK);
    }

    /**
     * Shows a simple warning message with an OK button.
     * @param header
     * @param message
     */
    public static void showWarning(String header, String message) {
        showMessage(header,message, Alert.AlertType.WARNING,MessageBoxChoices.OK);
    }

    /**
     * Shows a simple confirmation dialog with OK and CANCEL options.
     * @param header
     * @param message
     * @return
     */
    public static boolean showConfirmation(String header,String message) {
        return showMessage(header,message, Alert.AlertType.CONFIRMATION,MessageBoxChoices.OK,MessageBoxChoices.CANCEL)==MessageBoxChoices.OK;
    }

    /**
     * Shows a simple YES/NO/CANCEL dialog.
     * @param header
     * @param message
     * @return
     */
    public static MessageBoxChoices showYesNoCancel(String header,String message) {
        return
                showMessage(
                        header,
                        message,
                        Alert.AlertType.CONFIRMATION,
                        MessageBoxChoices.YES, MessageBoxChoices.NO, MessageBoxChoices.CANCEL
                );
    }


    /**
     * Returns a filename string without the extension.  A file extension is any sequence of
     * characters that follows a period, unless the period is the first character.
     * @param filename The filename of interest.
     * @return The filename without the extension, or the original value if there is no period after the first position.
     */
    public static String removeExtension(String filename) {
        int i = filename.lastIndexOf('.');
        return i<=0 ? filename : filename.substring(0,i);
    }

    public static String newExtension(String filename,String extension) {
        return removeExtension(filename)+extension;
    }

    /**
     * Removes unprintable characters, then encodes ? / :
     * as ##0xFF## where FF is the hex code from the ascii table
     * @param s
     * @return
     */
    public static String encodeIllegalFileCharacters(String s) {

        StringBuffer to = new StringBuffer();

        for (char c : s.toCharArray()) {
            switch (c) {
                case '?':
                case ':':
                case '/':
                case '"':
                    to.append(String.format("##0x%02X##",(int)c));
                    break;
                default:
                    to.append(c);
            }
        }
        return to.toString();
    }

    /**
     * Parses a string of hexadecimal characters and converts it to an int.
     * There should be no prefix like 0x.  For example an input value of
     * "FF" or "ff" returns 255.
     * @param s  The hex string to parse.
     * @return
     */
    public static int parseHex(String s) {
        int result=0;
        for (char c : s.toUpperCase().toCharArray()) {
            result = result << 4;
            if (c>='0' && c<='9')
                result += (int)c - (int)'0';
            else if (c>='A' && c<='F')
                result += 10 + (int)c - (int)'A';
            else
                throw new Error("Not a hexit: "+c);
        }
        return result;
    }

    final static Pattern codedCharacterPattern = Pattern.compile("^(.*)##0x([0-9A-F][0-9A-F])##(.*)$");

    public static String decodeIllegalFileCharacters(String s) {


        do {
            Matcher m = codedCharacterPattern.matcher(s);
            if (!m.matches()) break;
            s=m.group(1)+Character.toString(parseHex(m.group(2)))+m.group(3);
        } while (true);

        return s;
    }


    /**
     * <p>Parses a string for all reasonable forms of a "true" or "false" value. Comparison is
     * not case senstive:</p>
     * <UL>
     *   <li><b>TRUE</b>: any one of {@code "true", "1", "on", "yes", "t"}
     *   <li><b>FALSE</b>: any one of {@code "false", "0","off", "no", "f"}
     * </UL>
     * <p>U_Exception error codes:</p>
     * @param s The string to parse.
     * @return The boolean value of s, or {@code null} if it can't be parsed.
     */
    public static Boolean parseBoolean(String s) throws Exception {
        if (s==null)
            return null;
        String S=s.trim().toLowerCase();
        switch (S) {
            case "true":
            case "1":
            case "on":
            case "yes":
            case "t":
                return true;
            case "false":
            case "0":
            case "off":
            case "no":
            case "f":
                return false;
            default:
                return null;
        }
    }

    public static String nw(String s) {
        return s==null?"":s;
    }

    public static String nwp(String s) {
        return s==null?"(null)":s;
    }

    static private Pattern RE_SEQUENCE_INTEGER_LETTER = Pattern.compile("^(\\d+)([a-z])$");
    static private Pattern RE_SEQUENCE_INTEGER_DASH_INTEGER = Pattern.compile("^(\\d+)-(\\d+)$");

    /**
     * Normalizes strings to a standard sequencing format.
     *
     * <UL>
     *     <LI>All white space is trimmed first.</LI>
     *     <LI>If it parses as an integer, then it is reformatted using Integer.toString().</LI>
     *     <LI>If it parses as an integer followed by a letter, then it is reformatted
     *         as Integer.toString() followed by the character in lower case.</LI>
     *     <LI>If it parses as a Roman numeral, then it is normalized to upper case
     *         canonical Roman numeral format. Only the numbers 1 to 10 are
     *         supported.</LI>
     *     <LI>If it is a single alphabetic letter, it is converted to lower case.</LI>
     *     <LI>The original trimmed string is returned if none of the above apply.</LI>
     * </UL>
     * @param s
     * @return
     */
    public static String sequencify(String s) {
        String original=s;
        s=s.trim().toLowerCase();

        try {
            int i = Integer.parseInt(s);
            return Integer.toString(i);
        } catch (NumberFormatException e) {}


        Matcher m = RE_SEQUENCE_INTEGER_LETTER.matcher(s);
        if (m.matches()) {
            try {
                return Integer.toString((Integer.parseInt(m.group(1)))) +  m.group(2);
            } catch(NumberFormatException e) {}
        };

        if (s.length()==1 &&  Character.isAlphabetic(s.charAt(0)))
            return s;

        switch (s) {
            case "i":
            case "ii":
            case "iii":
            case "iv":
            case "v":
            case"vi":
            case "vii":
            case "viii":
            case "ix":
            case "x":
                return s.toUpperCase();
        }
        return original.trim();
    }


    /**
     * Returns an integer value assigned as the sort order for a sequence string as follows:
     *
     * <OL>
     *     <LI>If it parses as a single lower-case alphabetic character, then it gets the value
     *     from 0 to 25, where 0=a, 1=b, ...</LI>
     *     <LI>If it parses as an integer, then it is the value parsed shifted left 9 bits.</LI>
     *     <LI>If it parses as an integer followed by an upper or lower case letter, then
     *     the integer and letter values are placed into their respective fields as above, and
     *     upper case letters are treated the same as lower case letters.</LI>
     *     <LI>If the sequence is a Roman numeral from 1 to 10, meaning it is UPPER CASE
     *     "I".."X" then it is assigned the integer 1..10 shifted left 5 bits.
     *     <LI>If it conforms to the pattern "integer-integer" as in "127-128" then
     *     the first integer in the list is returned.</LI>
     *     <LI>If it is a sequence of 10 or fewer letters and digits, then a value that will
     *     sort in the same order as a case-insensitive string comparison will be returned.</LI>
     *     <LI>Anything else is give the value Long.MAX_VALUE</LI>
     * </OL>
     *
     *
     <PRE>
     __ __ __     __ __ __ __ __ __ __ __ __ __ __
     63 62 61 ... 10 09 08 07 06 05 04 03 02 01 00
     __ _______________ ___________ ______________
     0    integer         roman       letter
     unsigned      1=I .. 10=X   0=a...25=z

     __   __ __ __ __ __ __   __ __ __ __ __ __   ...   __ __ __ __ __ __    __ __ __
     63   62 61 60 59 58 57   56 55 54 53 52 51         08 07 06 05 04 03    02 01 00
     __   _________________   _________________   ...   _________________    ________
     1   first character      second character         tenth character       unused
     0=a..25=z,26='0'..35='9'
     </PRE>
     * @return
     */
    public static long sequenceSortOrder(String s) {
        s = s.trim().toLowerCase();

        try {
            long l = Long.parseLong(s);
            return l << 9;
        } catch (NumberFormatException e) {
        }

        Matcher m = RE_SEQUENCE_INTEGER_LETTER.matcher(s);
        if (m.matches()) {
            try {
                long l = Long.parseLong(m.group(1));
                int c = m.group(2).charAt(0) - 'a';
                return (l << 9) + c;
            } catch (NumberFormatException e) {
            }
        }

        m = RE_SEQUENCE_INTEGER_DASH_INTEGER.matcher(s);
        if (m.matches()) {
            try {
                return Long.parseLong(m.group(1)) << 9;
            } catch (NumberFormatException e) {}
        }

        if (s.length() == 1 && Character.isAlphabetic(s.charAt(0)))
            return s.charAt(0) - 'a';

        switch (s) {
            case "i":
                return 1 << 5;
            case "ii":
                return 2 << 5;
            case "iii":
                return 3 << 5;
            case "iv":
                return 4 << 5;
            case "v":
                return 5 << 5;
            case "vi":
                return 6 << 5;
            case "vii":
                return 7 << 5;
            case "viii":
                return 8 << 5;
            case "ix":
                return 9 << 5;
            case "x":
                return 10 << 5;
        }

        if (s.length() <= 10) {
            long val= 1L << 63;
            char sarr[] = s.toCharArray();
            int sarrlen = s.length();
            char c;
            long component;
            for (int x = 0; x<sarrlen; x++) {
                c = sarr[x];
                if (c>='a' && c<='z') {
                    component=c-'a';
                } else if (c>='0' && c<='9') {
                    component=c-'0'+26;
                } else
                    return Long.MAX_VALUE;
                val = val | (component << (  ((9-x)*6) + 3));
            }
            return val;
        }

        return Long.MAX_VALUE;
    }

    public static String  xmlDocumentToString(Document doc) throws TransformerException {
        TransformerFactory tf = TransformerFactory.newInstance();
        Transformer transformer = tf.newTransformer();
        transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no");
        transformer.setOutputProperty(OutputKeys.METHOD, "xml");
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
        transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
        transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4");
        try (StringWriter sw = new StringWriter()) {
            transformer.transform(new DOMSource(doc),
                    new StreamResult(sw));
            return sw.toString();
        } catch (Exception e) {
            return "Unable to transform XML";
        }
    }

    public static void printXmlDocument(Document doc, OutputStream out) throws IOException, TransformerException {
        TransformerFactory tf = TransformerFactory.newInstance();
        Transformer transformer = tf.newTransformer();
        transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no");
        transformer.setOutputProperty(OutputKeys.METHOD, "xml");
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
        transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
        transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4");

        transformer.transform(new DOMSource(doc),
                new StreamResult(new OutputStreamWriter(out, "UTF-8")));
    }

    public static String baseFileName(String path) {
        return baseFileName(new File(path));
    }

    public static String baseFileName(File file) {
        return removeExtension(file.getName());
    }

    public static Optional<Double> parseDouble(String s) {
        try {
            double d = Double.parseDouble(s);
            return Optional.of(d);
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    public static Optional<Integer> parseInteger(String s) {
        try {
            int i = Integer.parseInt(s);
            return Optional.of(i);
        } catch (Exception e) {
            return Optional.empty();
        }
    }
    public static String drainInputStream(InputStream s, int maxchars) throws IOException {
        StringBuffer sb = new StringBuffer(maxchars);

        BufferedReader reader = new BufferedReader(new InputStreamReader(s));
        String l=null;
        while ( (l = reader.readLine()) != null) {
            if (sb.length()>0)
                sb.append("\n");
            sb.append(l);
            if (sb.length()>=maxchars) {
                sb.setLength(maxchars);
                break;
            }
        }
        return sb.toString();
    }

    /**
     * Runs an external program and returns a {@code String} of the standard output stream.
     *
     * @param cmdarray An array of {@code String} values, the first of which is the command to
     * execute, with subsequent array values being the arguments to be passed to the command.
     *
     * @return The resulting stdout.  Output of the command may be truncated if it
     * is longer than an internally set maximum value.
     *
     * @throws Exception which includes the content of the standard error (if there is any) as part of
     * the exception message.
     */
    public static void launchExteranalProgram (String[] cmdarray)
            throws Exception
    {
        if (cmdarray.length<=0)
            throw new Exception("Launch of external program failed - empty command array");
        try {
            Process p = Runtime.getRuntime().exec(cmdarray);
        } catch (Exception e) {
            throw new Exception(String.format("Launch of external program '%s' failed",cmdarray[0]),e);
        }
    }


    /**
     * Determines if a string is composed of characters between ASCII value
     * 32 (space) and 126 (~, tilde).
     * @param s The string to be checked.
     * @return True if the string comprises only characters in the ASCII range 32..126.
     */
    public static boolean isPrintableCharacters(String s) {
        for (char c : s.toCharArray()) {
            if (c<32 || c>126)
                return false;
        }
        return true;
    }

    /**
     * Cleans up a title as follows:
     * <ol>
     *     <li>Removes leading sequencing information by matching a regular expression</li>
     * </ol>
     * @param title
     * @return
     */
    public static String cleanUpTitle(String title) {
        if (title==null)
            return "";
        Matcher m = titleCleanupPattern.matcher(title);
        if (m.matches())
            return m.group(2);
        return null;
    }
    private static Pattern titleCleanupPattern = Pattern.compile("^\\s*(\\d+[a-z]?\\s*-?\\s*){0,2}(.*)$");



    /**
     * Returns the handles directory path, which should be evaluated relative to SMB_ROOT
     * @param handle
     * @return A string value which is the relative path to the handle's directory.
     */
    /**
     * Returns the handles directory path, which should be evaluated relative to SMB_ROOT
     * @param handle
     * @return A string value which is the relative path to the handle's directory.
     */
    public static String pathToSmbHandleDir(int handle) {
        //Create links in the handle links area
        //
        //       HANDLE is  AB,CDE,FGH
        //
        //   smb/handles/E/D/C/B/A/handle
        //
        StringBuffer sb = new StringBuffer("handles/E/D/C/B/A");

        int ptr = sb.indexOf("E/");
        for (int count=0; count<5; count++) {
            sb.setCharAt(ptr,(char)('0'+handle%10));
            handle /= 10;
            ptr += 2;
        }

        return sb.toString();
    }

    public static String encodeUrl(String url) {
        url=url.replace("%","%25");
        url=url.replace(" ","%20");
        url=url.replace("+","%2B");
        return url;
    }

    public static Map<String,String> parseMetaFile(String metaFilePath) throws Exception {
        File metaFile = new File(metaFilePath);
        if (!metaFile.exists())
            throw new Exception("No meta.txt file found at " + metaFilePath);

        class CraigError extends Error {
            public CraigError(String message) {
                super(message);
            }
        }

        Pattern metaPattern = Pattern.compile("^([^=]+)=([^=]+)$");
        Map<String,String> map = new HashMap<>();

        try (java.io.BufferedReader reader = new BufferedReader(new FileReader(metaFile))) {
            //The first line should be the sentinel
            String meta = reader.readLine().trim();
            if (!meta.equals("[meta]"))
                throw new Exception("Meta file in " + metaFilePath + " first line must be '[meta]'");
            //subsequent lines are NameValuePairs
            final Integer[] lineNo = new Integer[]{1};
            reader.lines().forEach(line -> {
                lineNo[0]++;
                if (!line.trim().equals("")) {  //Skip any empty lines
                    Matcher matcher = metaPattern.matcher(line);
                    if (!matcher.matches())
                        throw new CraigError(String.format("%s line %d: Not a well-formed name=value pair", metaFile.getAbsolutePath(), lineNo[0]));
                    //TODO: Validate name=value here
                    map.put(matcher.group(1), matcher.group(2));
                }
            });
        } catch (CraigError e) {
            throw new Exception(e.getMessage());
        }
        return map;
    }

    private static SimpleDateFormat yearPattern = new SimpleDateFormat("yyyy");
    public static int getYear(java.util.Date date) {
        return Integer.parseInt(yearPattern.format(date));
    }

    private static Pattern patternYYYYMMDD = Pattern.compile("^(\\d{4,4})[-/](\\d{1,2})[-/](\\d{1,2})$");
    private static Pattern patternMMDDYYYY = Pattern.compile("^(\\d{1,2})[-/](\\d{1,2})[-/](\\d{4,4})$");

    public enum DateFormats {YYYYMMDD, MMDDYYYY, MMDDYY};

    public static java.util.Date parseDate(String dateString, DateFormats format) throws Exception {
        if (dateString==null)
            return null;
        try {
            int mgroup, dgroup, ygroup;
            Pattern pattern;
            switch (format) {
                case YYYYMMDD:
                    ygroup=1;
                    mgroup=2;
                    dgroup=3;
                    pattern=patternYYYYMMDD;
                    break;
                case MMDDYYYY:
                    mgroup=1;
                    dgroup=2;
                    ygroup=3;
                    pattern=patternMMDDYYYY;
                    break;
                default:
                    throw new Exception("Unknown format enum value");
            }
            Matcher matcher = pattern.matcher(dateString);
            if (!matcher.matches())
                throw new Exception(String.format("Can't parse %s as %s",dateString,format.toString()));
            int month = Integer.parseInt(matcher.group(mgroup));
            int day = Integer.parseInt(matcher.group(dgroup));
            int year = Integer.parseInt(matcher.group(ygroup));
            return Date.from(LocalDate.of(year,month,day).atStartOfDay(ZoneId.of("GMT")).toInstant());
                    } catch (Exception e) {
            throw new Exception("Could not parse "+dateString,e);
        }
    }

    public static Path blobDirectory(Path root, String blobkey) {
         return root.resolve(blobkey.substring(1,2)).resolve(blobkey.substring(3,4)).resolve(blobkey.substring(5,6));
    }



}

