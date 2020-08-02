package com.craiglowery.java.vlib.clients.editor;

import com.craiglowery.java.common.Util;
import com.craiglowery.java.vlib.clients.core.Video;
import javafx.fxml.Initializable;
import javafx.scene.control.Button;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextField;
import javafx.scene.control.cell.PropertyValueFactory;

import java.net.URL;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Created by craig on 5/21/2017.
 */
public class MassTitleEditorController implements Initializable {

    public TextField tfRegularExpression;
    public TextField tfReplacementExpression;
    public Button buttonOK;
    public Button buttonCancel;
    public Button buttonClear;
    public TableView<TitleComparator> tvComparison;
    public Button buttonUndo;
    public Button buttonApply;

    List<List<TitleComparator>> undoStack=new ArrayList<>();

    Map<Long,Video> map = null;

    @Override
    public void initialize(URL fxmlFileLocation, ResourceBundle resources) {
        buttonCancel.setCancelButton(true);
        buttonCancel.setOnAction( event -> {
            buttonCancel.getScene().getWindow().hide();
        });
        buttonOK.setDefaultButton(true);
        buttonOK.setOnAction( event -> {
            update();
            buttonOK.getScene().getWindow().hide();
        });
        buttonClear.setOnAction(event-> {
            tfRegularExpression.setText("");
            tfReplacementExpression.setText("");
        });
        buttonUndo.setOnAction(event -> {
            undo();
        });
        buttonApply.setOnAction(event -> apply());
        TableColumn<TitleComparator,Long> handleCol = new TableColumn<>("Handle");
        handleCol.setCellValueFactory(new PropertyValueFactory("handle"));
        TableColumn<TitleComparator,String> originalCol = new TableColumn<>("Original Title");
        originalCol.setCellValueFactory(new PropertyValueFactory("original"));
        TableColumn<TitleComparator,String> revisedCol = new TableColumn<>("Revised Title");
        revisedCol.setCellValueFactory(new PropertyValueFactory("revised"));
        TableColumn<TitleComparator,String> flagCol = new TableColumn<>("Flag");
        flagCol.setCellValueFactory(new PropertyValueFactory("flag"));

        tvComparison.getColumns().setAll(flagCol,handleCol,originalCol,revisedCol);
    }

    /**
     * Sets the dialog to edit titles for the specified collection of videos.
     * @param vc Collectgion of videos to which we should apply the mass title update.
     */
    public void set(Collection<Video> vc) {
        //Remember the parameters and important sizes for later use
        map = new HashMap<>();
        tvComparison.getItems().clear();
        undoStack.clear();
        for (Video v : vc) {
            tvComparison.getItems().add(new TitleComparator(v.getHandle(), v.getTitle()));
            map.put(v.getHandle(),v);
        }
    }

    private void pushState() {
        List<TitleComparator> ll  = new LinkedList<>();
        for (TitleComparator tc : tvComparison.getItems())
            ll.add(tc.clone());
        undoStack.add(ll);
    }

    private void popState() {
        int sz = undoStack.size();
        if (sz>0) {
            List<TitleComparator> ll = undoStack.get(sz-1);
            undoStack.remove(sz-1);
            tvComparison.getItems().clear();
            for (TitleComparator tc : ll)
                tvComparison.getItems().add(tc);
        }
    }

    private boolean apply() {
        //Try to compile the regular expression
        pushState();
        try {
            Pattern pattern = Pattern.compile(tfRegularExpression.getText());
            Pattern fixpat = Pattern.compile("\\{[1-9][0-9]?\\}");
            for (TitleComparator tc : tvComparison.getItems()) {
                Matcher m = pattern.matcher(tc.getOriginal());
                if (m.matches()) {
                    String r = tfReplacementExpression.getText();
                    for (int x=1; x<=m.groupCount(); x++) {
                        if (m.group(x)!=null)
                            r=r.replace("{"+x+"}",m.group(x));
                    }
                    r = String.join("",fixpat.split(r));
                    tc.setRevised(r);
                }
            }
        } catch (PatternSyntaxException e) {
            Util.showError("Regular Expression Error","The regular expression does not compile: "+e.getMessage());
            tvComparison.refresh();
            return false;
        }
        tvComparison.refresh();
        return true;
    }

    private void undo() {
        popState();
    }

    private void update() {
        for (TitleComparator tc : tvComparison.getItems()) {
            if (tc.changed()) {
                Video v = map.get(tc.getHandle());
                if (v == null)
                    Util.showError("Internal Programming Error","Could not find video for handle " + tc.getHandle());
                else
                    v.setTitle(tc.getRevised());
            }
        }
        tvComparison.getItems().clear();
        buttonOK.getScene().getWindow().hide();
    }


}
