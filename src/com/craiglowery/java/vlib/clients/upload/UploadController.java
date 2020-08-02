package com.craiglowery.java.vlib.clients.upload;

import com.craiglowery.java.common.Util;
import com.craiglowery.java.vlib.clients.core.NameValuePair;
import com.craiglowery.java.vlib.clients.core.Tag;
import com.craiglowery.java.vlib.clients.server.connector.*;
import javafx.animation.KeyFrame;
import javafx.animation.Timeline;
import javafx.application.Platform;
import javafx.collections.FXCollections;
import javafx.collections.ListChangeListener;
import javafx.collections.ObservableList;
import javafx.concurrent.WorkerStateEvent;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.fxml.Initializable;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.input.Dragboard;
import javafx.scene.input.KeyEvent;
import javafx.scene.input.MouseEvent;
import javafx.scene.input.TransferMode;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.text.Text;
import javafx.stage.Stage;
import javafx.stage.Window;
import javafx.stage.WindowEvent;
import javafx.util.Duration;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.File;
import java.net.URL;
import java.nio.file.Files;
import java.util.*;
import java.util.function.Consumer;
import java.util.prefs.BackingStoreException;
import java.util.prefs.Preferences;

import static com.craiglowery.java.vlib.clients.upload.InferencePattern.makeMap;

/**
 * Created by Craig on 2/3/2016.
 */
public class UploadController implements Initializable {
    @FXML public TextField textField_Filename;
    @FXML public TextField textField_Title;
    @FXML public TextField textField_Handle;
    @FXML public CheckBox checkBox_DuplicateCheck;
    @FXML public Button button_Upload;
    @FXML public Button button_Search;
    @FXML public Button button_Skip;
    @FXML public ProgressIndicator progressIndicator_Upload;
    @FXML public Text label_Status;
    @FXML public TextArea textArea_Log;
    @FXML public ListView<NameValuePair> listView_Pairs;
    @FXML public Button button_Add;
    @FXML public Button button_Clear;
    @FXML public Button button_Infer;
    @FXML public ListView<Tag> listView_TagNames;
    @FXML public ListView<String> listView_Values;
    @FXML public SplitPane gridPane_ROOT;
    @FXML public Button button_Remove;
    @FXML public ProgressIndicator progressIndicator_Tagop;
    @FXML public TextField textField_Value;
    @FXML public Button button_ForceAdd;
    @FXML public Label label_HandleRefersToTitle;
    @FXML public Label lbConnectedTo;
    @FXML public ChoiceBox<InferencePattern> cBox_InferValues;
    @FXML public Button button_NextOnQueue;
    @FXML public CheckBox checkBox_AutoSearch;
    @FXML public HBox hbox_ConnectedNext;
    @FXML public Button button_PromoteTitle;
    @FXML public Button button_CleanTitle;

    private ServerConnector serverConnector;
    private Preferences prefs;

    private ObservableList<File> fileQueue;

    private Set<Button> allButtons;


    Timeline timeline_HandleWatcher = null;
    @Override
    public void initialize(URL fxmlFileLocation, ResourceBundle resources) {

        fileQueue = FXCollections.observableArrayList();

        fileQueue.addListener(new ListChangeListener<File>() {
            @Override
            public void onChanged(Change<? extends File> c) {
                button_NextOnQueue.setVisible(fileQueue.size()!=0);
                button_NextOnQueue.setText(String.format("Get next of %d more",fileQueue.size()));
            }
        });

        checkBox_AutoSearch.setSelected(false);
        button_NextOnQueue.setVisible(false);
        progressIndicator_Upload.setVisible(false);
        hbox_ConnectedNext.setVisible(true);
        progressIndicator_Tagop.setVisible(false);
        try {
            ConnectionChooserController.Profile profile = ConnectionChooserController.chooseProfile();
            if (profile==null)
                Platform.exit();
            serverConnector = new ServerConnector(profile.baseUrl,profile.username,profile.password);
            lbConnectedTo.setText("Connected to: "+profile.name);

        } catch (ServerException e) {
            showMessage("Error initializing server connector: "+e.getMessage());
        }
        gridPane_ROOT.setOnDragOver(event -> {
            Dragboard db = event.getDragboard();
            if (db.hasFiles()) {
                event.acceptTransferModes(TransferMode.ANY);
            } else {
                event.consume();
            }
        });

        List<InferencePattern> iplist = cBox_InferValues.getItems();
        iplist.add(
                new InferencePattern("SEE - title","^\\s*([0-9])([0-9][0-9])\\s*-\\s*.*$",
                makeMap(1,"Season",2,"Episode")));
        iplist.add(
                new InferencePattern("SEE - SEQ - title","^\\s*([0-9])([0-9][0-9])\\s*-\\s*([0-9]+)\\s*-\\s*.*$",
                        makeMap(1,"Season",2,"Episode",3,"Sequence")));
        iplist.add(
                new InferencePattern("SEE - SEQ title","^\\s*([0-9])([0-9][0-9])\\s*-\\s*([0-9]+)\\s*.*$",
            makeMap(1,"Season",2,"Episode",3,"Sequence")));
        iplist.add(
                new InferencePattern("SEQ - title","^\\s*([0-9]+)\\s*-\\s*.*$",
                        makeMap(1,"Season",2,"Episode",3,"Sequence")));
        iplist.add(
                new InferencePattern("SEQ - SEE - title","^\\s*([0-9]+)\\s*-\\s*([0-9]){1,2}([0-9][0-9]).*$",
                        makeMap(1,"Sequence",2,"Season",3,"Episode")));

        iplist.add(
                new InferencePattern("S##E## - title","^S(\\d+)E(\\d+)\\s*-\\s*(.*)$",
                        makeMap(1,"Season",2,"Episode",3,"title")));

        iplist.add(
                new InferencePattern("title - S##E##","^(.*?)\\s*-\\s*S(\\d+)E(\\d+)$",
                        makeMap(1,"title",2,"Season",3,"Episode")));


        gridPane_ROOT.setOnDragDropped(event -> {
            Dragboard db = event.getDragboard();
            boolean success = false;
            boolean first=true;
            if (db.hasFiles()) {
                success = true;
                for (File file : db.getFiles()) {
                    if (first && fileQueue.size()==0) {
                        first=false;
                        teeUpFile(file);
                    } else {
                        fileQueue.add(file);
                    }
                }
            }
            event.setDropCompleted(success);
            event.consume();
        });

        allButtons = makeButtonSet(
                button_PromoteTitle,
                button_CleanTitle,
                button_Upload,
                button_Search,
                button_NextOnQueue,
                button_Skip,
                button_Add,
                button_Remove,
                button_Clear,
                button_ForceAdd,
                button_Infer);

        for (Button b : allButtons)
            b.setOnAction(event -> processButtonAction(b));

        listView_Values.setOnMouseClicked(event -> valuesMouseEventHandler(event));

        timeline_HandleWatcher =
                new Timeline(
                        new KeyFrame(
                                Duration.millis(1),
                                (action) -> {
                                    if (!textField_Handle.getText().trim().equals(""))
                                        button_Upload.setDisable(true);
                                }

                        ),
                        new KeyFrame(
                                Duration.millis(1500),
                                (action) -> {
                                    if (!textField_Handle.getText().trim().equals(""))
                                        lookupHandle();
                                }
                        )
                );
        timeline_HandleWatcher.setCycleCount(1);
        textField_Handle.textProperty().addListener((observable1, oldValue1, newValue1) -> {
            timeline_HandleWatcher.playFromStart();
        });

        listView_TagNames.getSelectionModel().setSelectionMode(SelectionMode.SINGLE);
        listView_Values.getSelectionModel().setSelectionMode(SelectionMode.MULTIPLE);
        listView_Pairs.getSelectionModel().setSelectionMode(SelectionMode.MULTIPLE);

        listView_Pairs.setOnKeyReleased(event -> keyModifyPair(event));

        retrieveTagNames();

        listView_TagNames.getSelectionModel().selectedItemProperty().addListener(
          (observable, oldValue, newValue) -> retrievTagValues(listView_TagNames.getSelectionModel().getSelectedItem()));
        prefs = Preferences.userRoot().node(this.getClass().getPackage().getName());
        loadPairsPrefrences();
    }

    private  Set<Button> makeButtonSet(Button...buttons) {
        Set<Button> bset = new HashSet<>();
        for (Button b : buttons)
            bset.add(b);

        return bset;
    }

    /***
     * Enables all buttons in the set, and disables the others.
     * @param buttons
     */
    private void enableOnlyButtons(Set<Button> buttons) {
        for (Button b : allButtons)
            b.setDisable(!buttons.contains(b));
    }

    private void enableButtons(Button...buttons) {
        for (Button b : buttons)
            b.setDisable(false);
    }
    private void disbleButtons(Button...buttons) {
        for (Button b : buttons)
            b.setDisable(true);
    }

    private void processButtonAction(Button button) {
        if (button==button_PromoteTitle) {
            textField_Title.setText(label_HandleRefersToTitle.getText());
        } else if (button==button_CleanTitle) {
            textField_Title.setText(Util.cleanUpTitle(textField_Title.getText()));
        } else if (button==button_Upload) {
            performUpload();
        } else if (button==button_Skip) {
            performSkip();
        } else if (button==button_NextOnQueue) {
            teeUpNextFile();
        } else if (button==button_Search) {
            performSearch(null);
        } else if (button==button_Infer) {
            inferValues();
        } else if (button==button_Add) {
            addTag();
        } else if (button==button_Remove) {
            removeTag();
        } else if (button==button_Clear) {
            clearTags();
        } else if (button==button_ForceAdd) {
            forceAdd();
        } else
            Util.showError("Error in Programming","Button has not been wired");
    }

    private void performSkip() {
        //disableButtons(button_Upload,button_NextOnQueue,button_Skip,button_Search);
        if (fileQueue.size()>0) {
            teeUpFile(fileQueue.remove(0));
            enableButtons(button_Search);
        }
    }

    private void teeUpFile(File file) {
        String filePath = file.getAbsolutePath();
        textField_Filename.setText(filePath);
        textField_Title.setText(Util.baseFileName(filePath));
        textField_Handle.setText("");
        if (checkBox_AutoSearch.isSelected()) {
            disbleButtons(button_Search, button_Upload, button_Skip);
            performSearch(textField_Title.getText());
        } else
            enableButtons(button_Search, button_Upload, button_Skip);
    }


    private void teeUpNextFile() {
        if (fileQueue.size()>0) {
            teeUpFile(fileQueue.remove(0));
        }
    }

    public void postStagingInitialization() {
        Window window = gridPane_ROOT.getScene().getWindow();


        Util.parseDouble(prefs.get("WindowWidth",null)).ifPresent(window::setWidth);
        Util.parseDouble(prefs.get("WindowHeight",null)).ifPresent(window::setHeight);
        Util.parseDouble(prefs.get("WindowX",null)).ifPresent(window::setX);
        Util.parseDouble(prefs.get("WindowY",null)).ifPresent(window::setY);

        window.heightProperty().addListener((observable, oldValue, newValue) -> prefs.put("WindowHeight", String.valueOf(newValue)));
        window.widthProperty().addListener((observable, oldValue, newValue) -> prefs.put("WindowWidth", String.valueOf(newValue)));
        window.xProperty().addListener((observable, oldValue, newValue) -> prefs.put("WindowX", String.valueOf(newValue)));
        window.yProperty().addListener((observable, oldValue, newValue) -> prefs.put("WindowY", String.valueOf(newValue)));

        window.setOnCloseRequest(this::handleCloseRequest);
    }

    private void handleCloseRequest(WindowEvent event) {
        storePairsPreferences();
    }

    private boolean loadPairsPrefrences() {
        try {
            List<NameValuePair> pairs = listView_Pairs.getItems();
            pairs.clear();
            Preferences pairsPrefs = prefs.node("pairs");
            for (String name : pairsPrefs.keys()) {
                String value = pairsPrefs.get(name, null);
                //Vet values here
                if (value!=null)
                    pairs.add(new NameValuePair(name,value));
            }
        } catch (BackingStoreException e) {
            return false;
        }
        return true;
    }

    private boolean storePairsPreferences() {
        //Save current profile
        try {
            Preferences pairs = prefs.node("pairs");
            pairs.clear();
            for (NameValuePair pair : listView_Pairs.getItems()) {
                pairs.put(pair.name,pair.value);
            }
        } catch (BackingStoreException e) {
            return false;
        }
        return true;

    }

    private void keyModifyPair(KeyEvent event) {
        switch (event.getCode()) {
            case ADD:
            case PLUS: modifyPair(1); break;
            case SUBTRACT:
            case MINUS: modifyPair(-1); break;
            default: break;
        }
    }


    private void modifyPair(int inc) {
        if (listView_Pairs.getSelectionModel().getSelectedItems().size()!=1) {
            showMessage("Must select exactly one pair to use key-modify function");
        }
        NameValuePair pair = listView_Pairs.getSelectionModel().getSelectedItem();

        if (pair!=null)
            try {
                NameValuePair newpair=new NameValuePair(pair.name,Integer.toString(Integer.parseInt(pair.value) + inc));
                int x = listView_Pairs.getSelectionModel().getSelectedIndex();
                listView_Pairs.getItems().set(x,newpair);
            }catch (Exception e ) {
            }
    }

    private void forceAdd() {
        //If there is a category selected and the value field is non-empty
        if (listView_TagNames.getSelectionModel().isEmpty()) return;
        String value = textField_Value.getText().trim();
        if (!validValue(value)) {
            showMessage("The value is not suitable for use as a tagging value.");
            return;
        }
        //If the value is in the value list
        if (listView_Values.getItems().contains(value)) {
            listView_Values.getSelectionModel().select(value);
            addTag();
            textField_Value.setText("");
            return;
        }
        //Force add to the list
        if (confirmYesOrNo("Force add the value?"))
            listView_Pairs.getItems().add(new NameValuePair(listView_TagNames.getSelectionModel().getSelectedItem().name,value));
    }
    private void inferValues(){
        //Match title against the selected inference pattern
        InferencePattern pattern = (InferencePattern)cBox_InferValues.getSelectionModel().getSelectedItem();
        Map<String,String> result = pattern.match(textField_Title.getText());
        List<NameValuePair> list = listView_Pairs.getItems();
        //Remove any tags that may have been returned
        List<NameValuePair> pairsToDelete =  new LinkedList<>();
        for (String name : result.keySet()) {
            for (NameValuePair pair : list)
                if (pair.name.equals(name))
                    pairsToDelete.add(pair);
        }
        for (NameValuePair pair : pairsToDelete)
            list.remove(pair);
        //Add back in new pairs
        for (String name : result.keySet()) {
            String value=result.get(name);
            try {
                int iValue = Integer.parseInt(value);
                value=Integer.toString(iValue);
            } catch(NumberFormatException e) {}
            if (name.equals("title"))
                textField_Title.setText(value);
            else {
                NameValuePair pair = new NameValuePair(name, value);
                list.add(pair);
            }
        }

    }

    private boolean validValue(String value) {
        for (char c : value.toCharArray())
            if (c<32 || c>126)
                return false;
        return true;
    }


    private void valuesMouseEventHandler(MouseEvent event) {
        if (event.getClickCount()==2)
            addTag();
    }

    private void setUploadControlsEnabled(boolean enabled) {
        button_Upload.setDisable(!enabled);
        button_Skip.setDisable(!enabled);
        button_NextOnQueue.setDisable(!enabled);
        button_Search.setDisable(!enabled);
        textField_Filename.setDisable(!enabled);
        textField_Handle.setDisable(!enabled);
        textField_Title.setDisable(!enabled);
        checkBox_DuplicateCheck.setDisable(!enabled);
        setTaggingControlsEnabled(enabled);
    }

    private void status(String msg) {
        label_Status.setText(msg);
    }

    private boolean confirmYesOrNo(String msg) {
        Alert dialog = new Alert(Alert.AlertType.CONFIRMATION,msg,ButtonType.YES,ButtonType.NO);
        Optional<ButtonType> result = dialog.showAndWait();
        return(result.isPresent() && result.get() == ButtonType.YES);
    }

    private void showMessage(String msg) {
        Alert dialog = new Alert(Alert.AlertType.INFORMATION,msg,ButtonType.OK);
        dialog.showAndWait();
    }


    private void lookupHandle() {
        // Examine handle value - must be non-negative integer
        int handle=-1;
        try {
            handle=Integer.parseUnsignedInt(textField_Handle.getText());
        } catch (NumberFormatException e) {
            label_HandleRefersToTitle.setText("Invalid handle");
            return;
        }
        // Create query string
        String select = "handle, title";
        String where = String.format("handle=%d",handle);
        String orderBy = null;
        // Set up success and failure routines

        Consumer<Document> success = ( document ) -> {
            //There may be 0 or 1 response, so either there is a title in here somewhere or there isn't
            XPath xp = XPathFactory.newInstance().newXPath();
            try {
                Element elTitle = (Element)xp.compile("//title").evaluate(document, XPathConstants.NODE);
                if (elTitle==null)
                    label_HandleRefersToTitle.setText("No such handle");
                else {
                    label_HandleRefersToTitle.setText(elTitle.getTextContent());
                    //Now try to get the tags
                    clearTags();
                    Element elTags = (Element)xp.compile("//tags").evaluate(document, XPathConstants.NODE);
                    if (elTags!=null) {
                        NodeList nl = (NodeList)xp.compile("tag").evaluate(elTags, XPathConstants.NODESET);
                        for (int x=0; x<nl.getLength(); x++) {
                            Element elTag = (Element)(nl.item(x));
                            String name = elTag.getAttribute("name");
                            String value = elTag.getAttribute("value");
                            if (name!=null && !name.equals("")) {
                                NameValuePair pair = new NameValuePair(name,value);
                                List<NameValuePair> list = listView_Pairs.getItems();
                                if (!list.contains(pair))
                                    list.add(pair);
                            }
                        }
                    }
                }
            } catch (XPathExpressionException e) {
                label_HandleRefersToTitle.setText("XPath Expression Error");
            }
            button_Upload.setDisable(false);
        };

        Consumer<Throwable> failure = ( thrown ) -> {
            if (thrown==null)
                label_HandleRefersToTitle.setText("Query error - no information available");
            else
                label_HandleRefersToTitle.setText("Query error: "+thrown.getMessage());
        };

        // Kick it off
        serverConnector.callAsyncQuery(select,where,orderBy,success,failure);
    }

    private void performUpload()
    {
        //See if MIME type can be determined
        String filename = textField_Filename.getText();
        String contenttype=null;
        try {
            contenttype = Files.probeContentType(new File(filename).toPath());
        } catch (Exception e) {
            showMessage(String.format("Could not determine MIME type for %s (Exception %s)",
                    filename,e.getMessage()));
            return;
        }
        if (contenttype==null) {
            showMessage(String.format("Could not determine MIME type for %s (probe returned null)",
                    filename));
        }


        textArea_Log.clear();
        //Check if there are tags defined
        if (listView_Pairs.getItems().size()==0 && !confirmYesOrNo("There are no tag assignments. Upload anyway?"))
                return;

        //Disable the upload button and all the fields
        setUploadControlsEnabled(false);

        //Show the progress indicator in an indeterminate state
        progressIndicator_Upload.setVisible(true);
        progressIndicator_Upload.setProgress(-1);
        hbox_ConnectedNext.setVisible(false);

        //Create the task
        ServerConnectorTask<ServerResponse<UploadResult>> uploadTask =
                serverConnector.createFileUploadTask(
                        textField_Filename.getText(),
                        textField_Title.getText(),
                        contenttype,
                        textField_Handle.getText(),
                        checkBox_DuplicateCheck.isSelected(),
                        listView_Pairs.getItems(),
                        true  /* autocreate */);

        //Set up a listener for progress bar of the task to update once a second
        Timeline progressPoll = new Timeline(new KeyFrame(Duration.seconds(1), new EventHandler<ActionEvent>() {
            @Override
            public void handle(ActionEvent event) {
                progressIndicator_Upload.setProgress(uploadTask.getProgress());
                status(uploadTask.getMessage());
            }
        }));
        progressPoll.setCycleCount(Timeline.INDEFINITE);

        //Set up listeners for when the background thread completes.
        java.util.function.BiConsumer<WorkerStateEvent,Boolean>cleanup = (event, succeeded) -> {
            progressPoll.stop();
            progressIndicator_Upload.setVisible(false);
            progressIndicator_Upload.setProgress(-1);
            hbox_ConnectedNext.setVisible(true);
            if (succeeded) {
                ServerResponse<UploadResult> response = (ServerResponse<UploadResult>)uploadTask.getValue();
                status(response.getResult().statusLine());
                StringBuffer sb = new StringBuffer(response.getResult().toString()).append("\n\n-----LOG-----\n\n")
                        .append(response.getLog().toString());
                textArea_Log.setText(sb.toString());
            } else {
                Throwable e = uploadTask.getException();
                if (e!=null) {
                    status("Upload failed: " + e.getMessage());
                    if (e instanceof ServerException)
                        textArea_Log.setText(((ServerException)e).getLog());
                    else
                        textArea_Log.setText("No log is available");
                }
                else
                    status("Upload failed - unknown reason");
            }


            setUploadControlsEnabled(true);
            button_Upload.setDisable(succeeded);


        };
        uploadTask.setOnSucceeded(event -> cleanup.accept(event,true));
        uploadTask.setOnFailed(event -> cleanup.accept(event,false));

        //Schedule the task and let 'er run!
        Thread backgroundThread = new Thread(uploadTask);
        backgroundThread.setDaemon(true);
        backgroundThread.start();
        progressPoll.play();
    }


    private void performSearch(String title) {
        String filter=null;

        if (title==null)
            filter = Util.showTextInputDialog("Input Dialog","Enter Search Filter Expression","");
        else
            filter=title;

        if (filter==null)
            return;
        filter=filter.trim();
        if (filter.equals(""))
            return;
        if (filter.startsWith(":"))
            filter=String.format("\"%s\"~$title",filter.substring(1));
        textArea_Log.clear();
        //Disable the upload button and all the fields
        setUploadControlsEnabled(false);

        //Show the progress indicator in an indeterminate state
        progressIndicator_Upload.setVisible(true);
        progressIndicator_Upload.setProgress(-1);
        hbox_ConnectedNext.setVisible(false);

        //Create the task
        ServerConnectorTask<ServerResponse<Document>> searchTask =
                title==null
                        ?
                serverConnector.createQueryTask("handle, title",filter,"title asc")
                        :
                serverConnector.createTitleHashTask("handle, title",filter,"title asc");

        //Set up a listener for progress bar of the task to update once a second
        Timeline progressPoll = new Timeline(new KeyFrame(Duration.seconds(1), new EventHandler<ActionEvent>() {
            @Override
            public void handle(ActionEvent event) {
                progressIndicator_Upload.setProgress(searchTask.getProgress());
                status(searchTask.getMessage());
            }
        }));
        progressPoll.setCycleCount(Timeline.INDEFINITE);

        //Set up listeners for when the background thread completes.
        java.util.function.BiConsumer<WorkerStateEvent,Boolean>cleanup = (event, succeeded) -> {
            progressPoll.stop();
            progressIndicator_Upload.setVisible(false);
            progressIndicator_Upload.setProgress(-1);
            hbox_ConnectedNext.setVisible(true);
            if (succeeded) {
                ServerResponse<Document> response = searchTask.getValue();
                status("Query succeeded");
                StringBuffer sb = new StringBuffer(response.getResult().toString()).append("\n\n-----LOG-----\n\n")
                        .append(response.getLog().toString());
                textArea_Log.setText(sb.toString());
                //Show the list of retrieved titles
                Document doc = response.getResult();
                if (doc==null) {
                    Util.showWarning("Result Set Empty","No objects were returned by the query.");
                } else {
                    Integer handle = selectHandleFromQueryResponse(response.getResult());
                    if (handle != null)
                        textField_Handle.setText(String.valueOf(handle));
                }
            } else {
                Throwable e = searchTask.getException();
                if (e!=null) {
                    status("Search failed: " + e.getMessage());
                    if (e instanceof ServerException)
                        textArea_Log.setText(((ServerException)e).getLog());
                    else
                        textArea_Log.setText("No log is available");
                }
                else
                    status("Search failed - unknown reason");
            }


            setUploadControlsEnabled(true);


        };
        searchTask.setOnSucceeded(event -> cleanup.accept(event,true));
        searchTask.setOnFailed(event -> cleanup.accept(event,false));

        //Schedule the task and let 'er run!
        Thread backgroundThread = new Thread(searchTask);
        backgroundThread.setDaemon(true);
        backgroundThread.start();
        progressPoll.play();

    }


    private Integer selectHandleFromQueryResponse(Document doc) {
        Stage stageQuerySelect = new Stage();
        SelectFromQueryController controller = null;
            try {
                URL fxml = getClass().getResource("SelectFromQuery.fxml");
                FXMLLoader fxmlLoader = new FXMLLoader(fxml);
                GridPane pane = fxmlLoader.load();
                controller = fxmlLoader.getController();
                Scene scene = new Scene(pane);
                stageQuerySelect = new Stage();
                stageQuerySelect.setScene(scene);
                stageQuerySelect.setTitle("Select Object to Retrieve Handle");
                controller.loadDocument(doc);
                controller.autoShowAndWait(stageQuerySelect);
                int handle=controller.getHandle();
                return handle==0?null:handle;
            } catch (Exception e) {
                showMessage("Unable to load title editor: "+e.getMessage());
            }

        return null;
    }

    private void setTaggingControlsEnabled(boolean enabled) {
        listView_TagNames.setDisable(!enabled);
        listView_Values.setDisable(!enabled);
        listView_TagNames.setDisable(!enabled);
        button_Add.setDisable(!enabled);
        button_Clear.setDisable(!enabled);
        button_Remove.setDisable(!enabled);
        progressIndicator_Tagop.setProgress(-1);
        button_Infer.setDisable(!enabled);
        progressIndicator_Tagop.setVisible(!enabled);
    }



    private void retrieveTagNames() {
        //Disable the tagging controls.  This also acts like a mutex for all
        //the tagging actions.
        listView_TagNames.setDisable(true);

         //Create the task
        ServerConnectorTask<ServerResponse<ObservableList<Tag>>> tagGettingTask =
                serverConnector.createTagNameGettingTask();

        //Set up listeners for when the background thread completes.
        java.util.function.BiConsumer<WorkerStateEvent,Boolean>cleanup = (event, succeeded) -> {
            if (succeeded) {
                ServerResponse<ObservableList<Tag>> response = tagGettingTask.getValue();
                listView_TagNames.setItems(response.getResult());
                listView_TagNames.getItems().sort(tagComparator);
                textArea_Log.setText(response.getLog());
            } else {
                Throwable e = tagGettingTask.getException();
                if (e!=null) {
                    status("Unable to get tags: " + e.getMessage());
                    if (e instanceof ServerException)
                        textArea_Log.setText(((ServerException)e).getLog());
                    else
                        textArea_Log.setText("No log is available");
                }
                else
                    status("Tag retrieval failed - unknown reason");
            }

            setTaggingControlsEnabled(true);
        };

        tagGettingTask.setOnSucceeded(event -> cleanup.accept(event,true));
        tagGettingTask.setOnFailed(event -> cleanup.accept(event,false));

        //Schedule the task and let 'er run!
        Thread backgroundThread = new Thread(tagGettingTask);
        backgroundThread.setDaemon(true);
        backgroundThread.start();
    }

    private void retrievTagValues(Tag tag) {
        if (tag==null)
            return;
        //Disable the tagging controls.  This also acts like a mutex for all
        //the tagging actions.
        listView_TagNames.setDisable(true);

        //Create the task
        ServerConnectorTask<ServerResponse<ObservableList<String>>> tagValueGettingTask =
                serverConnector.createTagValueGettingTask(tag.name);

        //Set up listeners for when the background thread completes.
        java.util.function.BiConsumer<WorkerStateEvent,Boolean>cleanup = (event, succeeded) -> {
            if (succeeded) {
                ServerResponse<ObservableList<String>> response = tagValueGettingTask.getValue();
                listView_Values.setItems(response.getResult());
                listView_Values.getItems().sort(stringComparator);
                textArea_Log.setText(response.getLog());
                reconcileVisibleTagValues();
            } else {
                Throwable e = tagValueGettingTask.getException();
                if (e!=null) {
                    status("Unable to get tag values for tag "+tag.name+": " + e.getMessage());
                    if (e instanceof ServerException)
                        textArea_Log.setText(((ServerException)e).getLog());
                    else
                        textArea_Log.setText("No log is available");
                }
                else
                    status("Tag valueretrieval failed - unknown reason");
            }

            setTaggingControlsEnabled(true);
        };

        tagValueGettingTask.setOnSucceeded(event -> cleanup.accept(event,true));
        tagValueGettingTask.setOnFailed(event -> cleanup.accept(event,false));

        //Schedule the task and let 'er run!
        Thread backgroundThread = new Thread(tagValueGettingTask);
        backgroundThread.setDaemon(true);
        backgroundThread.start();
    }


    private void addTag() {
        // Both a tag name and at least one tag value must be selected
        if (listView_TagNames.getSelectionModel().isEmpty() ||
            listView_Values.getSelectionModel().isEmpty())
            return;
        String name = listView_TagNames.getSelectionModel().getSelectedItem().name;
        for ( String value : listView_Values.getSelectionModel().getSelectedItems()) {
            NameValuePair pair = new NameValuePair(name,value);
            List<NameValuePair> list = listView_Pairs.getItems();
            if (!list.contains(pair))
                list.add(pair);
        }
        listView_Values.getSelectionModel().clearSelection();
        reconcileVisibleTagValues();
    }


    Comparator<String> stringComparator = new Comparator<String>() {
        public int compare(String a, String b) {
           return a.compareTo(b);
        };
    };

    Comparator<Tag> tagComparator = new Comparator<Tag>() {
        public int compare(Tag a, Tag b) {
            return a.name.compareTo(b.name);
        };
    };


    private void removeTag() {
        if (listView_Pairs.getSelectionModel().isEmpty())
            return;
        List<NameValuePair> selected =
            new LinkedList<>(listView_Pairs.getSelectionModel().getSelectedItems());
        Tag selectedTag = listView_TagNames.getSelectionModel().getSelectedItem();
        for (NameValuePair pair : selected) {
            listView_Pairs.getItems().remove(pair);
            if (selectedTag!=null && selectedTag.name.equals(pair.name) &&
                    !listView_Values.getItems().contains(pair.value)) {
                listView_Values.getItems().add(pair.value);
            }
        }
        sortList();
    }

    private void sortList() {
        listView_Values.getItems().sort(stringComparator);
    }

    private void clearTags() {
        listView_Pairs.getSelectionModel().selectAll();
        removeTag();
    }

    private void reconcileVisibleTagValues() {
        Tag tag = listView_TagNames.getSelectionModel().getSelectedItem();
        if (tag==null) {
            listView_Values.getItems().clear();
            return;
        }
        for (NameValuePair pair : listView_Pairs.getItems())
            if (pair.name.equals(tag.name) && listView_Values.getItems().contains(pair.value))
                listView_Values.getItems().remove(pair.value);
        sortList();
    }

}
