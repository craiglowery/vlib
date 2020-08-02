package com.craiglowery.java.vlib.clients.editor;


import com.craiglowery.java.common.Util;
import com.craiglowery.java.vlib.clients.core.NameValuePair;
import com.craiglowery.java.vlib.clients.core.Tag;
import com.craiglowery.java.vlib.clients.core.Video;
import com.craiglowery.java.vlib.clients.upload.TagValueCoverageButton;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.Node;
import javafx.scene.control.Button;
import javafx.scene.control.TextField;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.VBox;

import java.net.URL;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.ResourceBundle;

/**
 * Created by Craig on 2/3/2016.
 */
public class TagAssignmentEditorController implements Initializable {

    @FXML public GridPane gpRoot;
    @FXML public Button btClear;
    @FXML public Button btCancel;
    @FXML public Button btOk;
    @FXML public VBox vbTags;
    @FXML public Button btAdd;
    @FXML public TextField tfValue;
    @FXML public Button btAutoNumber;

    private Collection<Video> videoCollection;
    private Tag tag;
    private int numberOfVideos;

    @Override
    public void initialize(URL fxmlFileLocation, ResourceBundle resources) {
        btClear.setOnAction( event -> {
           for (Node n : vbTags.getChildren())
               ((TagValueCoverageButton)n).setCoverage(TagValueCoverageButton.Coverage.NONE);
        });
        btCancel.setCancelButton(true);
        btCancel.setOnAction( event -> {
            btCancel.getScene().getWindow().hide();
        });
        btOk.setDefaultButton(true);
        btOk.setOnAction( event -> {
            update();
            btOk.getScene().getWindow().hide();
        });

        btAdd.setOnAction( event -> addValue() );

        btAutoNumber.setOnAction( event -> autoNumber() );
    }

    /**
     * For a given tag and set of video objects, populates the dialog with a set of buttons,
     * one for each value of the tag.  Each button's color represents it's value's coverage
     * over the set of videos:<p>
     *     <DL>
     *         <DT>ALL
     *         <DD>Every video in the collection is tagged with this value.
     *         <DT>SOME</DT>
     *         <DD>At least one, but not all of the videos in the collection are tagged with this value.</DD>
     *         <DT>NONE</DT>
     *         <DD>None of the videos in the collection are tagged with this value.</DD>
     *     </DL>
     * @param vc  The set of videos.
     * @param t The tag.
     */
    public void set(Collection<Video> vc, Tag t) {
        //Remember the parameters and important sizes for later use
        videoCollection=vc;
        tag=t;
        numberOfVideos=vc.size();

        //Get a ref to the list where the buttons will be placed after creation
        ObservableList<Node> ta = vbTags.getChildren();

        //Remove any existing buttons
        ta.clear();

        //Reset the value add controls
        tfValue.setText("");

        //Create a map for each value, that we use to count how many times (how many videos)
        //we see it.  If a value isn't in the map, then the count is effectively 0.
        Map<String /*tag value*/, Integer /*times seen*/> cocount = new HashMap<>();

        //Look at each video and count the tag values
        for (Video v : videoCollection) {
            //For each tag assignment in this video
            for (NameValuePair nvp : v.getTags())
                //If the tag name for this pair is the tag we care about...
                if (nvp.name.equals(tag.name)) {
                    //Get the current count and increment it.
                    Integer i = cocount.get(nvp.value);
                    cocount.put(nvp.value, i==null ? 1 : (i+1));
                }
        }

        //Now create buttons for the values and set their coverage accordingly
        for (String value : t.getValues()) {
            //Assume the coverage is NONE until proven otherwise
            TagValueCoverageButton.Coverage coverage = TagValueCoverageButton.Coverage.NONE;
            Integer i = cocount.get(value);
            if (i!=null)
                coverage = i==numberOfVideos ? TagValueCoverageButton.Coverage.ALL : TagValueCoverageButton.Coverage.SOME;
            vbTags.getChildren().add(new TagValueCoverageButton(value,coverage));
        }
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
        //Go through all the buttons
        for (Node N : vbTags.getChildren()) {
            //Just making sure this is a special button - precaution
            if (N instanceof TagValueCoverageButton) {
                //Get a reference to the button, and to it's value
                TagValueCoverageButton tta = (TagValueCoverageButton)N;
                String value = tta.getText();
                //If a value's coverage is "SOME" then skip it
                //If a value's coverage is "NONE" then remove it from every video
                //If a value's coverage is "ALL" then apply it to every video
                switch (tta.getCoverage()) {
                    case SOME:
                        continue;
                    case NONE:
                        for (Video v : videoCollection)
                            v.untag(tag.name,value);
                        break;
                    case ALL:
                        for (Video v : videoCollection)
                            try {
                                v.tag(tag.name, value);
                            } catch (Exception e) {}
                }
            }
        }
    }

    /**
     * Determines if the value proposed is valid, not a duplicate, then confirms addition to the list.
     */
    private void addValue() {
        String value = tfValue.getText().trim();
        tfValue.setText("");
        if (value.equals("")) {
            return;
        }
        //if this value is already in the list, then just select it
        //Go through all the buttons
        for (Node N : vbTags.getChildren()) {
            //Just making sure this is a special button - precaution
            if (N instanceof TagValueCoverageButton) {
                //Get a reference to the button, and to it's value
                TagValueCoverageButton tta = (TagValueCoverageButton)N;
                //If it is in the list already, then set that value to ALL and update
                if (value.equals(tta.getText())) {
                    tta.setCoverage(TagValueCoverageButton.Coverage.ALL);
                    update();
                    return;
                }
            }
        }
        //This is indeed a new value.  Is it a valid tag value? (Must be printable characters)
        if (!Util.isPrintableCharacters(value)) {
            Util.showError("Invalid Tag Value","Tag values must be non-empty strings of printable characters.");
            return;
        }
        //Add it to local information and set it for all
        TagValueCoverageButton button = new TagValueCoverageButton(value, TagValueCoverageButton.Coverage.ALL);
        //The buttons are in sorted order.  We will find where this one goes int he list and insert it
        //----sort insert logic should go here----
        vbTags.getChildren().add(button);
        tag.addValue(value);
        update();
    }

    private void autoNumber() {
        String startWith= Util.showTextInputDialog("Auto Number","Auto Number","Enter the starting integer value");
        if (startWith==null || startWith.trim().equals(""))
            return;
        int start=0;
        try {
            start=Integer.parseInt(startWith.trim());
        } catch (NumberFormatException e) {
            Util.showError("Input Error","Your input was not a parseable integer");
            return;
        }
        for (Video v : videoCollection) {
            try {
                String s = ""+start++;
                tag.addValue(s);
                v.tag(tag.name, s);
            } catch (Exception e) {
                Util.showError("Error","Unexpected exception while auto numbering: "+e.getMessage());
                return;
            }
        }
        btOk.getScene().getWindow().hide();
    }

}
