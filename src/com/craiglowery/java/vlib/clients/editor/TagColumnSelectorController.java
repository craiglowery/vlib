package com.craiglowery.java.vlib.clients.editor;


import com.craiglowery.java.vlib.clients.upload.TagValueCoverageButton;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.Node;
import javafx.scene.control.Button;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.VBox;

import java.net.URL;
import java.util.*;

/**
 * Created by Craig on 2/3/2016.
 */
public class TagColumnSelectorController implements Initializable {

    @FXML public GridPane gpRoot;
    @FXML public Button btClear;
    @FXML public Button btCancel;
    @FXML public Button btOk;
    @FXML public VBox vbTags;

    private boolean canceled =false;

    @Override
    public void initialize(URL fxmlFileLocation, ResourceBundle resources) {
        btClear.setOnAction( event -> {
        });

        btCancel.setCancelButton(true);
        btCancel.setOnAction( event -> {
            canceled =true;
            btCancel.getScene().getWindow().hide();
        });

        btOk.setDefaultButton(true);
        btOk.setOnAction( event -> {
            btOk.getScene().getWindow().hide();
        });

    }

    /**
     * Sets the dialog with the set of tag names and the subset that is currently being displayed.
     * @param allTagNames     The set of all tags.
     * @param tagsCurrentlyShownNames The set of tags currently displayed.
     */
    public void set(Collection<String> allTagNames, Collection<String> tagsCurrentlyShownNames) {
        canceled =false;
        ObservableList<Node> ta = vbTags.getChildren();
        ta.clear();

        //Create a sorted list adn populate the box
        String[] sortedTagNames = allTagNames.toArray(new String[0]);
        Arrays.sort(sortedTagNames);

        //Now populate the list with buttons
        for (String tagname : sortedTagNames) {
            if (!tagsCurrentlyShownNames.contains(tagname)) {
                TagValueCoverageButton b = new TagValueCoverageButton(
                tagname,
                TagValueCoverageButton.Coverage.NONE);
                vbTags.getChildren().add(b);
            }
        }

    }

    /**
     * Determins if the dialog was canceled.
     * @return True if the dialog was canceled.
     */
    public boolean wasCanceled() {
        return canceled;
    }


    public List<String> getTagsToAdd() {
        LinkedList<String> s = new LinkedList<>();
        for (Node n : vbTags.getChildren())
            if (TagValueCoverageButton.class.isAssignableFrom(n.getClass())) {
                TagValueCoverageButton b = (TagValueCoverageButton)n;
                if (b.getCoverage().equals(TagValueCoverageButton.Coverage.ALL))
                    s.add(b.getText());
            }
        return s;
    }

}
