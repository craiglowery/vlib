package com.craiglowery.java.vlib.clients.editor;


import com.craiglowery.java.vlib.clients.core.Video;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.Node;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.ListView;
import javafx.scene.control.TextField;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.scene.input.MouseButton;
import javafx.scene.input.MouseEvent;
import javafx.scene.layout.AnchorPane;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.stage.WindowEvent;
import org.apache.http.HttpStatus;
import org.json.JSONArray;
import org.json.JSONObject;


import java.net.URL;
import java.net.URLEncoder;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Craig on 1/26/2020
 */
public class IMdBlookupController implements Initializable {

    @FXML public BorderPane bpRoot;
    @FXML public Button btCancel;
    @FXML public Button btOk;
    @FXML public ListView lvChoices;


    Video video;
    boolean stripLeadingNumbers=false;

    @Override
    public void initialize(URL fxmlFileLocation, ResourceBundle resources) {
        btCancel.setCancelButton(true);
        btCancel.setOnAction( event -> {
            btCancel.getScene().getWindow().hide();
        });
        btOk.setDefaultButton(true);
        btOk.setOnAction( event -> {
            update();
            btOk.getScene().getWindow().hide();
        });

        lvChoices.getSelectionModel().selectedItemProperty().addListener(
                (object,oldValue,newValue) -> {
                    btOk.setDisable(false);
                }
        );


        String browserCmd = "\"C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe\"";

        lvChoices.setOnMouseReleased(
                (event) -> {
                    if (event.getButton()== MouseButton.SECONDARY) {
                        IMdBrecord record = (IMdBrecord)lvChoices.getSelectionModel().getSelectedItem();
                        try {
                            new Thread( () -> {
                                try {
                                    Runtime.getRuntime().exec(String.format("%s \"https://imdb.com/title/%s\"", browserCmd, record.imdbID));
                                } catch (Exception e) {}
                            }
                            ).start();
                        } catch (Exception e) {}

                    }
                }
        );

        lvChoices.setOnMouseClicked(
                (event) -> {
                    if (event.getClickCount() == 2) {
                        btOk.fire();
                    }
                }
        );

    }




    public ObservableList<IMdBrecord> performIMdBlookup(String title) throws Exception {
        /* Call IMDB for a search */
        String stitle=null;

        //Remove leading numbers if indicated
        if (stripLeadingNumbers) {
            Pattern pLeading = Pattern.compile("^\\s*\\d+\\s*-?\\s*\\d*\\s*(.+?)$");
            Matcher matcher = pLeading.matcher(title);
            if (matcher.matches())
                title=matcher.group(1);
        }

        //Remove any bracketed info
        Pattern pBrackets = Pattern.compile("^(.*?)\\s*\\[.*\\]\\s*$");
        Matcher matcher = pBrackets.matcher(title);
        if (matcher.matches()) {
            title = matcher.group(1);
        }

        //Get rid of any punctuation characters from the title
        StringBuffer santizedTitle = new StringBuffer();
        for (char c : title.toCharArray()) {
            if (Character.isAlphabetic(c) ||
                Character.isDigit(c) ||
                c==' ') {
            }
            santizedTitle.append(c);
        }
        title=santizedTitle.toString().replace("  "," ").replace("  "," ");
        try {
            stitle = URLEncoder.encode(title, "UTF-8");
        } catch (Exception e) {
            throw new RuntimeException("URL encoding failed (shouldn't happen "+e.getMessage(),e);
        }
        List<IMdBrecord> rval = new LinkedList<>();

        for (int page=1;  page<=10; page++) {
            String request = "https://movie-database-imdb-alternative.p.rapidapi.com/?page="+page+"&r=json&s=" + stitle;
            HttpResponse<JsonNode> response = Unirest.get(request)
                    .header("x-rapidapi-host", "movie-database-imdb-alternative.p.rapidapi.com")
                    .header("x-rapidapi-key", "5f8d21be91msh705bca2bdf41a30p1c15bcjsn4ce4f9bdca5e")
                    .asJson();


            if (response.getStatus() != HttpStatus.SC_OK) {
                throw new Exception("Request to IMdB failed - " + response.getStatusText());
            }

            JSONObject rootObject = response.getBody().getObject();
            String r = rootObject.get("Response").toString();
            if (r == null) {
                throw new Exception("Request to IMdB failed - the Response field was null");
            }
            if (!r.equals("True")) {
                break;
            }

            JSONArray resultsArray = (JSONArray) rootObject.get("Search");
            for (Object entry : resultsArray) {
                if (entry instanceof JSONObject) {
                    JSONObject map = (JSONObject) entry;
                    if (!map.get("Type").equals("game"))
                    rval.add(new IMdBrecord(
                            map.get("Title").toString(),
                            map.get("Year").toString(),
                            map.get("Poster").toString(),
                            map.get("Type").toString(),
                            map.get("imdbID").toString()
                    ));
                }
            }
        }

        if (rval.size()==0) {
            throw new Exception("Request to IMdB returned no entries for '"+title+"'");
        }

        return FXCollections.observableList(rval);
    }



    /**
     * Sets the dialog to do IMdB lookup for the specified video.
     * @param v Video of interest.
     */
    public void set(Video v, MouseEvent mouseEvent) throws Exception {
        btOk.setDisable(true);
        lvChoices.getItems().clear();
        video=v;
        stripLeadingNumbers =  (mouseEvent.isAltDown());
        lvChoices.setItems(performIMdBlookup(v.getTitle()));
    }




    /**
     * <p>Updates the video set with new tag assignments as set in the dialog according to the buttons
     * coverage settings.</p>
     *  <dl>
     *      <dt>ALL</dt>
     *      <dd>Every video will be tagged with the value.</dd>
     *      <dt>SOME</dt>
     *      <dd>Nothing will be changed</dd>
     *      <dt>NONE</dt>
     *      <dd>The value will be removed from any videos on which it currently occurs</dd>
     *  </dl>
     */
    public void update() {
        try {
            video.untag("Year");
            IMdBrecord selectedRecord = (IMdBrecord) lvChoices.getSelectionModel().getSelectedItem();
            video.tag("Year", selectedRecord.year);
            video.untag("Plex");
            video.tag("Plex","Yes");
            video.setTitle(selectedRecord.title);
        } catch (Exception e) {
            throw new RuntimeException("Unexpected error while updating video from IMdB",e);
        }
    }

}
