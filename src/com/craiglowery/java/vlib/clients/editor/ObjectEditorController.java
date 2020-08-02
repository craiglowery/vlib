package com.craiglowery.java.vlib.clients.editor;

import com.craiglowery.java.common.Util;
import com.craiglowery.java.vlib.clients.core.*;
import com.craiglowery.java.vlib.clients.server.connector.*;
import com.craiglowery.java.vlib.clients.upload.*;
import javafx.animation.Animation;
import javafx.animation.KeyFrame;
import javafx.animation.Timeline;
import javafx.application.Platform;
import javafx.beans.property.ReadOnlyLongWrapper;
import javafx.beans.property.ReadOnlyStringWrapper;
import javafx.beans.value.ObservableValue;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.concurrent.Task;
import javafx.concurrent.WorkerStateEvent;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.fxml.Initializable;
import javafx.scene.Node;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.scene.control.cell.TextFieldTableCell;
import javafx.scene.input.KeyCode;
import javafx.scene.input.MouseButton;
import javafx.scene.input.MouseEvent;
import javafx.scene.layout.AnchorPane;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.GridPane;
import javafx.stage.Stage;
import javafx.util.Duration;
import javafx.util.StringConverter;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URL;
import java.nio.file.Path;
import java.util.*;
import java.util.prefs.BackingStoreException;

import static javafx.scene.control.cell.TextFieldTableCell.forTableColumn;


/**
 * Created by Craig on 2/3/2016.
 */
public class ObjectEditorController implements Initializable, Observer {

    final int EXPRESSION_HISTORY_SIZE = 50;
    final String EXPRESSION_PREFERENCES_NAME = "Expressions";

    @FXML public ComboBox cbExpression;
    @FXML public GridPane gp_ROOT;
    @FXML public Button btLoad;
    @FXML public Button btRevert;
    @FXML public Button btUpdate;
    @FXML public Button btCancel;
    @FXML public TableView<Video> tvVideos;
    @FXML public Label lbStatus;
    @FXML public ProgressIndicator progressIndicator;
    @FXML public Button btAddTagColumn;
    @FXML public Label lbQuick;
    @FXML public Label lbNumberOfRecords;
    @FXML public Label lbModifiedRecords;
    @FXML public Label lbConnectedTo;


    private ServerConnector serverConnector;
    private VideoSchema schema = null;
    private TagDefinitions tagdefs = null;
    private Map<Long,Video> videos = new HashMap<>();
    private Set<String> tagsInUse = new HashSet();
    private TableView.TableViewSelectionModel<Video> tsm;
    private ObservableList<TablePosition> selectedCells;
    private boolean allowEditing = false;
    private ObservableList<String> olExpressions = FXCollections.observableArrayList();

    private Stage stageTagEditor=null;
    private TagAssignmentEditorController controllerTagEditor=null;

    private Stage stageTitleEditor=null;
    private TitleEditorController controllerTitleEditor=null;

    private Stage stageIMdBlookup=null;
    private IMdBlookupController controllerIMdBlookup=null;

    private Stage stageTagColumnSelector=null;
    private TagColumnSelectorController controllerTagColumnSelector=null;

    private ViewProfile<Video> columnOrdering = null;

    private boolean tableColumnsChanged=false;

    private PrefsManager prefsManager;

    @Override
    public void initialize(URL fxmlFileLocation, ResourceBundle resources) {
        progressIndicator.setVisible(true);
        try {
            ConnectionChooserController.Profile profile = ConnectionChooserController.chooseProfile();
            if (profile==null)
                Platform.exit();
            serverConnector = new ServerConnector(profile.baseUrl,profile.username,profile.password);
            lbConnectedTo.setText("Connected to: "+profile.name);
        } catch (ServerException e) {
            showMessage("Error initializing server connector: "+e.getMessage());
        }
        tvVideos.setPlaceholder(new Label("Loading..."));

        tsm = tvVideos.getSelectionModel();
        tsm.setSelectionMode(SelectionMode.MULTIPLE);
        tsm.setCellSelectionEnabled(true);

        try {
            columnOrdering = new ViewProfile<Video>("default");
        } catch (BackingStoreException e) {
            showMessage("Backing store exception");
        }

        cbExpression.setItems(olExpressions);
        cbExpression.setEditable(true);

        setStatus("Loading schema and tag definitions");
        btLoad.setDisable(true);
        btRevert.setDisable(true);
        btUpdate.setDisable(true);
        btCancel.setDisable(true);
        loadSchema();
        loadTagdefs();

        btLoad.setOnAction(
                event -> {
                    event.consume();
                    //Whatever string is showing in the combo box edit field, get it and trim it
                    String s = cbExpression.getValue().toString().trim();
                    //If that string is already in the list of expressions remove it (it will be re-added to the top)
                    if (olExpressions.contains(s))
                        olExpressions.remove(s);
                    //Add the expression to the top of the list
                    olExpressions.add(0,s);
                    //If the list has grown too big, remove the last one from the list
                    if (olExpressions.size()>EXPRESSION_HISTORY_SIZE)
                        olExpressions.remove(EXPRESSION_HISTORY_SIZE,olExpressions.size()-1);
                    doLoad(s);
                }
        );

        btUpdate.setOnAction(
                event -> {
                    event.consume();
                    doUpdate();
                }
        );

        btRevert.setOnAction(
                event -> {
                    doRevert();
                }
        );

        btAddTagColumn.setOnAction (
                event ->   modifyTagColumnsDisplayed()
        );

        //Wire the expression combo box to hit the Load button when ENTER is pressed
        cbExpression.setOnKeyPressed( event -> {
            if (event.getCode().equals(KeyCode.ENTER)) {
                event.consume();
                cbExpression.commitValue();
                if (!btLoad.isDisabled())
                    btLoad.fire();
                cbExpression.getSelectionModel().select(0);

            }
        });

        //Wire a keyboard listener and contextualize the input according to the type of the cell
        //currently selected
        tvVideos.setOnKeyReleased(

                (event) -> {
                    //See if it is UNDO
                    if (event.getCode().equals(KeyCode.Z) && event.isControlDown()) {
                        setStatus(Video.undo());
                        //This is an UNDO request
                    } else if (event.getCode().equals(KeyCode.DELETE)) {
                        //Clear all cells that are tag values
                        selectedCells = tsm.getSelectedCells();
                        for (TablePosition<Video,String> tp : selectedCells) {
                            String columnName = tp.getTableColumn().getText();
                            if (tagdefs.isTagName(columnName)) {
                                Video v = tvVideos.getItems().get(tp.getRow());
                                v.untag(columnName);
                            }
                        }
                    }
                    //We only handle keyboard input for one cell
                    else if (tsm.getSelectedCells().size() == 1) {
                        selectedCells = tsm.getSelectedCells();
                        TableColumn tc = selectedCells.get(0).getTableColumn();
                        //Is it a tag column?
                        String columnName = tc.getText();
                        int tr = selectedCells.get(0).getRow();
                        Video v = tvVideos.getItems().get(tr);
                        if (tagdefs.isTagName(columnName)) {
                            if (!v.isMultiValued(columnName)) {
                                final int INCREMENT = 1;
                                final int DECREMENT = 2;
                                final int EDIT = 3;
                                final int CLEAR = 4;
                                int action = 0;
                                if (event.getText().equals("+")) action=INCREMENT;
                                else if (event.getText().equals("-")) action=DECREMENT;
                                else if (event.getCode().equals(KeyCode.DELETE)) action=CLEAR;
                                else if (event.getCode().equals(KeyCode.F2)) action=EDIT;

                                switch (action) {
                                    case INCREMENT:
                                    case DECREMENT:
                                        //If it is a sequenced tag, increment or decrement the integer
                                        if (tagdefs.getTag(columnName).type == Tag.TagType.SEQUENCE) {
                                            String s = v.getTagValueString(columnName);
                                            try {
                                                long l = Long.parseLong(s);
                                                l += action == INCREMENT ? 1 : -1;
                                                v.untag(columnName);
                                                v.tag(columnName, Long.toString(l));
                                            } catch (Exception e) {
                                                //Can't parse as an integer or tag failed
                                            }
                                        }
                                        //If it is a category tag, cycle through the list
                                        else if (tagdefs.getTag(columnName).type == Tag.TagType.CATEGORY) {
                                            String currentValue = v.getTagValueString(columnName);
                                            Collection values = tagdefs.getTagValues(columnName);
                                            String[] aValues = (String[]) values.toArray(new String[0]);
                                            int marker = -1;
                                            for (int x = 0; x < values.size(); x++) {
                                                if (aValues[x].equals(currentValue)) {
                                                    marker = x;
                                                    break;
                                                }
                                            }
                                            v.untag(columnName);
                                            try {
                                                if (action == INCREMENT) {
                                                    marker++;
                                                    if (marker < aValues.length)
                                                        v.tag(columnName, aValues[marker]);
                                                } else {
                                                    marker--;
                                                    if (marker < 0)
                                                        marker = aValues.length - 1;
                                                    v.tag(columnName, aValues[marker]);
                                                }
                                            } catch (Exception e) {
                                                //Do nothing
                                            }
                                        }

                                        break;
                                    case EDIT:

                                        break;
                                }
                            }
                        }
                    }
                    resetButtons();

                }
        );


        progressIndicator.setVisible(false);

        //Set up a listener for progress bar of the task to update once a second
        Timeline updateProfileInterrupt = new Timeline(new KeyFrame(Duration.seconds(10), new EventHandler<ActionEvent>() {
            @Override
            public void handle(ActionEvent event) {
                if (tableColumnsChanged && columnOrdering!=null) {
                    tableColumnsChanged=false;
                    try {
                        columnOrdering.update(tvVideos);
                    } catch (BackingStoreException e) {}
                }
            }
        }));
        updateProfileInterrupt.setCycleCount(Timeline.INDEFINITE);
        updateProfileInterrupt.play();
        prefsManager = new PrefsManager(this,gp_ROOT, event -> {prefsManager.saveArray(olExpressions.toArray(new String[]{}),EXPRESSION_PREFERENCES_NAME);});
        olExpressions.addAll( prefsManager.loadArray(EXPRESSION_PREFERENCES_NAME));
    }


    /**
     * Recursively walks to scene graph rooted at node, looking for {@code TableHeaderRow}
     * nodes. When found, an event handler is added
     * @param node
     */
    public void wireColumnHeaders(Node node) {
        if ( node.getClass().getName().endsWith(".TableHeaderRow") ) {
            node.addEventFilter(MouseEvent.MOUSE_RELEASED, event -> tableColumnsChanged = true);
        }
        if (node instanceof Parent) {
            for (Node child : ((Parent) node).getChildrenUnmodifiable()) {
                wireColumnHeaders(child);
            }
        }
    }

    private void doUpdate() {

        LinkedList<ServerConnector.UpdateRequest> requests = new LinkedList<>();
        boolean anythingDirty=false;
        for (Video v : videos.values()) {
            //Each video is a potential call to update.
            HashSet<NameValuePair> tags = new HashSet<>();
            boolean dirty=false;
            if (v.hasDirtyAttributes()) {
                dirty=true;
                //Update attributes here
                tags.add(new NameValuePair("title",v.getTitle()));
            }
            if (v.hasDirtyTags()) {
                dirty=true;
                //Update tags here
                for ( NameValuePair nvp : v.getTags() )
                    tags.add(nvp);
            }
            if (dirty) {
                //We'll do the update if it is actually clean
                anythingDirty=true;
                ServerConnector.UpdateRequest request = serverConnector.createUpdateRequest(v.getHandle(),tags);
                requests.add(request);
            }
        }
        if (anythingDirty) {
            performUpdate(requests);
        } else {
            Util.showInformation("Nothing to Do","There are no pending changes");
            resetButtons();
        }
        refreshRecordCounts();
        Video.clearUndo();
    }


    private  boolean updateInProgress=false;

    private void performUpdate(Collection<ServerConnector.UpdateRequest> requests)
    {
        if (updateInProgress) {
            Util.showWarning("Warning","Update already in progress");
            return;
        }

        XPath xp = XPathFactory.newInstance().newXPath();
        updateInProgress=true;
        progressIndicator.setProgress(0);
        progressIndicator.setVisible(true);

        //Create the task
        ServerConnectorTask<ServerResponse<Document>> queryTask =
                serverConnector.createUpdateTask(requests);


        Timeline tl = new Timeline();
        tl.getKeyFrames().add(new KeyFrame(Duration.seconds(1), (event) -> progressIndicator.setProgress(queryTask.getProgress())));
        tl.setCycleCount(Animation.INDEFINITE);

        //Set up listeners for when the background thread completes.
        java.util.function.BiConsumer<WorkerStateEvent,Boolean>cleanup = (event, succeeded) -> {
            tl.stop();
            if (succeeded) {
                //Examine results document
                Document xml = queryTask.getValue().getResult();
                try {
                    Util.printXmlDocument(xml,System.err);
                    Element elReport = (Element)xp.evaluate("/update_report", xml, XPathConstants.NODE);
                    String s_succeededCount = elReport.getAttribute("succeeded");
                    String s_failedCount =  elReport.getAttribute("failed");
                    int succeededCount = Integer.valueOf(s_succeededCount);
                    int failedCount = Integer.valueOf(s_failedCount);
                    //Make all the successful handles clean
                    NodeList successes = (NodeList)xp.evaluate("/update_report/succeeded/handle",xml, XPathConstants.NODESET);
                    int x = successes.getLength();
                    for (int i=0; i<x; i++) {
                        String sh = ((Element)(successes.item(i))).getTextContent();
                        long handle = Long.valueOf(sh);
                        Video v = videos.get(handle);
                        if (v!=null)
                            v.makeClean();
                    }
                    if (failedCount>0)
                        Util.showError("Some Updates Failed",String.format("Succeeded: %d     Failed: %d",
                                succeededCount,failedCount));
                    else {
                        Util.showInformation("Updates Succeeded", String.format("%s objects were updated", succeededCount));
                    }
                    resetButtons();

                } catch (Exception e) {
                    Util.showWarning("Unexpected Error","Unable to parse update results.  Success of the update is unknown.");
                }
            } else {
                Throwable e = queryTask.getException();
                Util.showError("Unexpected Error During Update",e==null?"No more information is available":e.getMessage());
            }


            progressIndicator.setVisible(false);
            updateInProgress=false;
        };

        queryTask.setOnSucceeded(event -> cleanup.accept(event,true));
        queryTask.setOnFailed(event -> cleanup.accept(event,false));

        //We'll need a timeline to refresh the progress indicator
        tl.play();

        //Schedule the task and let 'er run!
        Thread backgroundThread = new Thread(queryTask);
        backgroundThread.setDaemon(true);
        backgroundThread.start();

    }


    public boolean isDirty() {
        for (Video v : videos.values())
            if (v.isDirty())
                return true;
        return false;
    }

    public void doRevert() {
        for (Video v : videos.values())
            if (v.isDirty())
                v.revert();
        refreshRecordCounts();
        resetButtons();
    }

    private void processEditRequest(MouseEvent mouseEvent) {
        if (!allowEditing)
            return;

        //Get selection and make sure it is non-empty
        selectedCells = tsm.getSelectedCells();
        int numSelectedCells = selectedCells.size();
        if (numSelectedCells<=0)
            return;

        //Are all cells in the same column?
        TableColumn commonTc = null;
        for (TablePosition tp : selectedCells) {
            TableColumn tc = tp.getTableColumn();
            if (commonTc == null)
                commonTc = tc;
            else if (commonTc != tc) {
                commonTc = null;
                break;
            }
        }

        if (commonTc!=null) {
            //They are all in the same column
            //Is this the title column?
            ObservableList<Video> allvids = tvVideos.getItems();

            if (commonTc.getText().toLowerCase().equals("*")) {
                //Have they selected the dirty indicator column? It is non-editable
                return;
            } else if (commonTc.getText().toLowerCase().equals("handle")) {
                //Have they selected the handle column?
                if (numSelectedCells==1) {
                    //If they selected a single handle and right-clicked, do IMDB lookup
                    TablePosition tp = selectedCells.get(0);
                    IMdBlookup(allvids.get(tp.getRow()),mouseEvent);
                } else {
                    showMessage("Can only perform IMdB lookup on one handle at a time");
                    return;
                }
            } else if (commonTc.getText().toLowerCase().equals("title")) {

                if (numSelectedCells==1) {
                    TablePosition tp = selectedCells.get(0);
                    editTitle(allvids.get(tp.getRow()));
                } else {
                    //Show mass title editor
                    LinkedList<Video> selectedvids = new LinkedList<>();
                    for (int x=0; x<numSelectedCells; x++)
                        selectedvids.add(allvids.get(selectedCells.get(x).getRow()));
                    massTitleEditor(selectedvids);

                }
            } else if (tagdefs.isTagName(commonTc.getText())) {
                LinkedList<Video> selectedvids = new LinkedList<>();
                for (int x=0; x<numSelectedCells; x++)
                    selectedvids.add(allvids.get(selectedCells.get(x).getRow()));
                editTags(selectedvids,tagdefs.getTag(commonTc.getText()));
            }
        } else {
            //They are in different columns
            showMessage("Cannot edit groups of cells from different columns");
        }
        refreshRecordCounts();
    }

    private void IMdBlookup(Video v, MouseEvent mouseEvent) {
        if (stageIMdBlookup==null) {
            try {
                URL fxml = getClass().getResource("IMdBlookup.fxml");
                FXMLLoader fxmlLoader = new FXMLLoader(fxml);
                BorderPane pane = fxmlLoader.load();
                controllerIMdBlookup = fxmlLoader.getController();
                Scene scene = new Scene(pane);
                stageIMdBlookup = new Stage();
                stageIMdBlookup.setScene(scene);
                stageIMdBlookup.setTitle("IMdB Lookup Results");
            } catch (Exception e) {
                showMessage("Unable to load IMdB Lookup: "+e.getMessage());
                return;
            }
        }

        try {
            controllerIMdBlookup.set(v,mouseEvent);
            stageIMdBlookup.showAndWait();
        } catch (Exception e) {
            showMessage("IMdB lookup failed - "+e.getMessage());
        }

        resetButtons();
    }


    private void editTitle(Video v) {
        if (stageTitleEditor==null) {
            try {
                URL fxml = getClass().getResource("TitleEditor.fxml");
                FXMLLoader fxmlLoader = new FXMLLoader(fxml);
                GridPane pane = fxmlLoader.load();
                controllerTitleEditor = fxmlLoader.getController();
                Scene scene = new Scene(pane);
                stageTitleEditor = new Stage();
                stageTitleEditor.setScene(scene);
                stageTitleEditor.setTitle("Title Editor");
            } catch (Exception e) {
                showMessage("Unable to load title editor: "+e.getMessage());
                return;
            }
        }

        controllerTitleEditor.set(v);
        stageTitleEditor.showAndWait();

        resetButtons();
    }

    private void modifyTagColumnsDisplayed() {
        if (stageTagColumnSelector==null) {
            try {
                URL fxml = getClass().getResource("TagColumnSelector.fxml");
                FXMLLoader fxmlLoader = new FXMLLoader(fxml);
                GridPane pane = fxmlLoader.load();
                controllerTagColumnSelector = fxmlLoader.getController();
                Scene scene = new Scene(pane);
                stageTagColumnSelector = new Stage();
                stageTagColumnSelector.setScene(scene);
                stageTagColumnSelector.setTitle("Tag Column Selector");
            } catch (Exception e) {
                showMessage("Unable to load tag column selector: "+e.getMessage());
                return;
            }
        }

        controllerTagColumnSelector.set(tagdefs.getTagNames(),tagsInUse);
        stageTagColumnSelector.showAndWait();

        if (!controllerTagColumnSelector.wasCanceled()) {
            Collection<String> columnsToKeep = controllerTagColumnSelector.getTagsToAdd();
            for (String c : columnsToKeep) {
                TableColumn<Video, String> newColumn = new TableColumn<>(c);
                newColumn.setCellValueFactory(
                                cellData -> {
                                    Video v = cellData.getValue();
                                    String display = v.getTagValueString(c);
                                    return new ReadOnlyStringWrapper(display==null?"":display);
                                }
                        );

                tvVideos.getColumns().add(newColumn);
            }
            wireColumnHeaders(tvVideos);
            tsm = tvVideos.getSelectionModel();
            tsm.setSelectionMode(SelectionMode.MULTIPLE);
            tsm.setCellSelectionEnabled(true);
        }
    }

    private void  editTags(Collection<Video> vc, Tag t) {
        if (stageTagEditor==null) {
            try {
                URL fxml = getClass().getResource("TagAssignmentEditor.fxml");
                FXMLLoader fxmlLoader = new FXMLLoader(fxml);
                GridPane pane = (GridPane)fxmlLoader.load();
                controllerTagEditor = fxmlLoader.getController();
                Scene scene = new Scene(pane);
                stageTagEditor = new Stage();
                stageTagEditor.setScene(scene);
                stageTagEditor.setTitle("Tag Assignment Editor");
            } catch (Exception e) {
                showMessage("Unable to load tag assignment editor: "+e.getMessage());
                return;
            }
        }

        controllerTagEditor.set(vc,t);
        stageTagEditor.showAndWait();
        resetButtons();
    }


    private void resetButtons(){
        boolean dirty=isDirty();
        btLoad.setDisable(dirty);
        btRevert.setDisable(!dirty);
        btUpdate.setDisable(!dirty);
        btCancel.setDisable(!dirty);
    }

    private void playVideo() {
        selectedCells = tsm.getSelectedCells();
        for (TablePosition tp : selectedCells) {
            int row = tp.getRow();
            ObservableList<Video> allvids = tvVideos.getItems();
            String[] cmdArray = new String[2];
            cmdArray[0]="c:\\Program Files (x86)\\SMPlayer\\smplayer.exe";
            //cmdArray[1] = getVideoAsURI(allvids.get(row).getHandle());
            cmdArray[1] = getVideoAsSMB(allvids.get(row).getHandle());
            if (cmdArray[1]==null)
                return;
            try {
                Util.launchExteranalProgram(cmdArray);
            } catch (Exception e) {
                Util.showError("Execution Error",e.getMessage());
            }
            return;
        }
    }

    private String getVideoAsURI(long handle) {
        return String.format(serverConnector.getROOTURI(true)+"/objects/%d/download",handle);
    }
    private String getVideoAsSMB(long handle) {
        Path smb_root = new File("//jclwhs/videos/smb").toPath();
        Path handle_dir = smb_root.resolve(Util.pathToSmbHandleDir((int)handle));
        Path ref_path = handle_dir.resolve(""+handle);
        try (BufferedReader br = new BufferedReader(new FileReader(ref_path.toFile()))) {
            String contentFileName = br.readLine();
            return handle_dir.resolve(contentFileName).toString();
        } catch (Exception e) {
            Util.showError("File Open Error","Unable to open reference file at "+ref_path.toString());
        }
        return null;
    }

    public void doLoad(String where) {
        if (where.length()==0) {
            showMessage("You must provide a filter expression");
            return;
        }
        btLoad.setDisable(true);
        setStatus("Querying server...");
        performQuery(null, where, null);
    }

    private void showMessage(String msg) {
        Alert dialog = new Alert(Alert.AlertType.INFORMATION,msg,ButtonType.OK);
        dialog.showAndWait();
    }

    private void checkReady() {
       if (schema!=null && tagdefs!=null) {
           btLoad.setDisable(false);
           setStatus("Schema and tagging definitions have been loaded successfully");
       }
    }

    private void setStatus(String s, Object ... args) {
       lbStatus.setText(String.format(s,args));
    }

    private void setPlaceholder(String s, Object ... args){
        tvVideos.setPlaceholder(new Label(String.format(s,args)));
    }

    private void  loadSchema() {
        //Create the task
        ServerConnectorTask<ServerResponse<Document>> loadSchemaTask =
                serverConnector.createSchemaTask();

        //Set up listeners for when the background thread completes.
        java.util.function.BiConsumer<WorkerStateEvent,Boolean>cleanup = (event, succeeded) -> {
            if (succeeded) {
                ServerResponse<Document> response = loadSchemaTask.getValue();
                try {
                    schema = new VideoSchema(response.getResult());
                    checkReady();
                } catch (Exception e) {
                    setStatus("Error parsing schema from XML document: %s",e.getMessage());
                }
            } else {
                Throwable e = loadSchemaTask.getException();
                if (e!=null) {
                   setStatus("Schema retrieval failed: %s",e.getMessage());
                }
                else
                   setStatus("Schema retrieval failed - no exception was returned");
            }


        };

        loadSchemaTask.setOnSucceeded(event -> cleanup.accept(event,true));
        loadSchemaTask.setOnFailed(event -> cleanup.accept(event,false));

        //Schedule the task and let 'er run!
        Thread backgroundThread = new Thread(loadSchemaTask);
        backgroundThread.setDaemon(true);
        backgroundThread.start();


    }

    private void loadTagdefs() {
        //Create the task
        ServerConnectorTask<ServerResponse<Document>> loadTagdefsTask =
                serverConnector.createTagDefinitionsGettingTask();

        //Set up listeners for when the background thread completes.
        java.util.function.BiConsumer<WorkerStateEvent,Boolean>cleanup = (event, succeeded) -> {
            if (succeeded) {
                ServerResponse<Document> response = loadTagdefsTask.getValue();
                try {
                    tagdefs = new TagDefinitions(response.getResult());
                    checkReady();
                } catch (Exception e) {
                    setStatus("Error parsing tag definitions from XML document: %s",e.getMessage());
                }
            } else {
                Throwable e = loadTagdefsTask.getException();
                if (e!=null) {
                    setStatus("Tag definitions retrieval failed: %s",e.getMessage());
                }
                else
                    setStatus("Tag definitions retrieval failed - no exception was returned");
            }


        };

        loadTagdefsTask.setOnSucceeded(event -> cleanup.accept(event,true));
        loadTagdefsTask.setOnFailed(event -> cleanup.accept(event,false));

        //Schedule the task and let 'er run!
        Thread backgroundThread = new Thread(loadTagdefsTask);
        backgroundThread.setDaemon(true);
        backgroundThread.start();
    }


    @Override
    public void update(Observable o, Object arg) {
        tvVideos.refresh();
    }

    private void loadTableView(Document doc) {
        setStatus("Parsing results");
        tvVideos.getItems().clear();
        videos.clear();
        Video.clearUndo();
        Observer observer = this;

        Task<Map<Long,Video>>  parseTask = new Task<Map<Long, Video>>() {
            @Override
            protected Map<Long, Video> call() throws Exception {
                HashMap<Long,Video> newVideos = new HashMap<>();
                XPath xp = XPathFactory.newInstance().newXPath();
                try {
                    NodeList objectsList = (NodeList) xp.compile("/result/query/objects/object").evaluate(doc, XPathConstants.NODESET);
                    int numObjects = objectsList.getLength();
                    for (int x = 0; x < numObjects; x++) {
                        Element elObject = (Element) objectsList.item(x);
                        //Add each attribute in the attributes list
                        Video v = new Video(elObject, schema, tagdefs);
                        newVideos.put(v.getHandle(), v);
                        updateProgress(x,numObjects);
                        v.addObserver(observer);
                    }
                } catch (Exception e) {
                    throw new Exception(String.format("Parse failed :%s",e.getMessage()));
                }
                return newVideos;
            }
        };


        //Set up a listener for progress bar of the task to update once a second
        Timeline progressPoll = new Timeline(new KeyFrame(Duration.seconds(1), new EventHandler<ActionEvent>() {
            @Override
            public void handle(ActionEvent event) {
                progressIndicator.setProgress(parseTask.getProgress());
            }
        }));
        progressPoll.setCycleCount(Timeline.INDEFINITE);

        parseTask.setOnSucceeded(event ->
        {
            progressPoll.stop();
            tvVideos.getColumns().clear();
            videos=parseTask.getValue();
            setColumns();
            columnOrdering.apply(tvVideos);
            btLoad.setDisable(false);
            setStatus("Ok");
            progressIndicator.setVisible(false);
            allowEditing = videos.size()>0;
            refreshRecordCounts();
        });

        parseTask.setOnFailed(event -> {
            progressPoll.stop();
            Throwable t = parseTask.getException();
            setStatus("Parsing failed: %s",t==null?"no exception was thrown":t.getMessage());
            progressIndicator.setVisible(false);
            refreshRecordCounts();
            }
        );

        parseTask.setOnCancelled(event -> {
            setStatus("Cancelled");
            refreshRecordCounts();
        });



        Thread backgroundThread = new Thread(parseTask);
        backgroundThread.setDaemon(true);
        backgroundThread.start();
        progressPoll.play();
    }

    /**
     * Creates columns:
     *   * Base attributes like "handle" based on what is configured
     *   * Tag columns based on which tags are used in the current set of videos
     */
    private void setColumns() {

        //Clear existing column definitions
        ObservableList<TableColumn<Video,?>> tableColumns = tvVideos.getColumns();
        tableColumns.clear();

        //Clear the items and reload with the current set of videos from most recent query
        tvVideos.getItems().clear();
        tvVideos.getItems().addAll(videos.values());

        //The first column is used to indicate when a row is "dirty" (needs to be updated)
        //It will show an asterisk when the row has been changed and has not yet been
        //posted back to the database.
        TableColumn<Video,String> dirtyCol = new TableColumn<>("*"); //The column header is the "*" symbol itself
        //Wire cells in this column the determine their value based on the "isDirty" value of the video
        dirtyCol.setCellValueFactory(
                cellData -> {
                    Video v = cellData.getValue();
                    return new ReadOnlyStringWrapper(v.isDirty()?"*":" ");
                }
        );
        tableColumns.add(dirtyCol);


        TableColumn<Video, Number> handleCol = new TableColumn<>("Handle");
        handleCol.setCellValueFactory(
                cellData -> {
                    Video v = cellData.getValue();
                    return new ReadOnlyLongWrapper(v.getHandle());
                }
        );
        tableColumns.add(handleCol);

        TableColumn<Video, String> titleCol = new TableColumn<>("Title");
        titleCol.setCellValueFactory(new PropertyValueFactory<>("title"));
        titleCol.setCellFactory(TextFieldTableCell.forTableColumn());

        tableColumns.add(titleCol);

        //Add columns for tags in use
        tagsInUse.clear();
        for (Video v : videos.values()) {
            for (NameValuePair p : v.getTags())
                if (!tagsInUse.contains(p.name)) {
                    String name = p.name;  //Needs to be isolated to this closure
                    TableColumn<Video,String> col = new TableColumn<>(p.name);
                    col.setCellValueFactory(
                            cellData -> {
                                String display = cellData.getValue().getTagValueString(name);
                                return new ReadOnlyStringWrapper(display==null?"":display);
                            }
                    );
                    tagsInUse.add(name);
                    Tag tag = tagdefs.getTag(name);
                    if (tag!=null) {
                        //Set comparator for sequence type tags
                        if (tag.type== Tag.TagType.SEQUENCE)
                        col.setComparator( (s1, s2) -> {
                            if (s1==s2) return 0; //both null or same object (not intended to compare string values)
                            if (s1==null) return -1;
                            if (s2==null) return 1;
                            return Long.compare(Util.sequenceSortOrder(s1), Util.sequenceSortOrder(s2));
                        });
                    }
                    tableColumns.add(col);
                }
        }

        tvVideos.setOnMouseClicked( event ->
            {
                if (event.getButton()==MouseButton.SECONDARY) {
                    event.consume();
                    processEditRequest(event);
                } else if (event.getButton().equals(MouseButton.PRIMARY) ) {
                    if (event.getClickCount()==2) {
                        playVideo();
                    }
                }
            }
        );

        tvVideos.layout();
        wireColumnHeaders(tvVideos);
    }


    private void updateProfile() {
        if (columnOrdering!=null)
            try {
                columnOrdering.update(tvVideos);
            } catch (BackingStoreException e) {}
    }

    private void performQuery(String select, String where, String orderby)
    {
        progressIndicator.setVisible(true);

        //Create the task
        ServerConnectorTask<ServerResponse<Document>> queryTask =
                serverConnector.createQueryTask(select,where,orderby);

        //Set up listeners for when the background thread completes.
        java.util.function.BiConsumer<WorkerStateEvent,Boolean>cleanup = (event, succeeded) -> {
            if (succeeded) {
                loadTableView(queryTask.getValue().getResult());
            } else {
                Throwable e = queryTask.getException();
                if (e!=null) {
                    setStatus("Query failed: %s", e.getMessage());
                }
                else
                    setStatus("Query failed - no exception was thrown");
                resetButtons();
            }


        };

        queryTask.setOnSucceeded(event -> cleanup.accept(event,true));
        queryTask.setOnFailed(event -> cleanup.accept(event,false));

        //Schedule the task and let 'er run!
        Thread backgroundThread = new Thread(queryTask);
        backgroundThread.setDaemon(true);
        backgroundThread.start();

    }

    private void refreshRecordCounts() {
        lbNumberOfRecords.setText(String.valueOf(videos.values().size()));
        lbModifiedRecords.setText(String.valueOf(countDirtyObjects()));
    }

    private int countDirtyObjects() {
        int dirty=0;
        for (Video v : videos.values())
            if (v.isDirty())
                dirty++;
        return dirty;
    }

    private Stage stageMassTitleEditor=null;
    private MassTitleEditorController controllerMassTitleEditor=null;
    private void massTitleEditor(Collection<Video> vc) {
        if (stageMassTitleEditor==null) {
            try {
                URL fxml = getClass().getResource("MassTitleEditor.fxml");
                FXMLLoader fxmlLoader = new FXMLLoader(fxml);
                AnchorPane pane = fxmlLoader.load();
                controllerMassTitleEditor = fxmlLoader.getController();
                Scene scene = new Scene(pane);
                stageMassTitleEditor = new Stage();
                stageMassTitleEditor.setScene(scene);
                stageMassTitleEditor.setTitle("Mass title Editor");
            } catch (Exception e) {
                showMessage("Unable to load mass title editor: "+e.getMessage());
                return;
            }
        }

        controllerMassTitleEditor.set(vc);
        stageMassTitleEditor.showAndWait();
        resetButtons();
        tvVideos.refresh();
    }

}
