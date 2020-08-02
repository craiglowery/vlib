package com.craiglowery.java.vlib.clients.upload;

import com.craiglowery.java.common.Util;
import javafx.beans.value.ChangeListener;
import javafx.beans.value.ObservableValue;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.geometry.Orientation;
import javafx.scene.control.ListView;
import javafx.scene.layout.GridPane;
import javafx.stage.Stage;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import java.net.URL;
import java.util.ResourceBundle;

/**
 * Created by craig on 11/4/2016.
 */
public class SelectFromQueryController implements Initializable {

    @FXML ListView<Entry> listView_Titles;
    @FXML GridPane gridPane_ROOT;

    private int saveHandle=0;

    public  int numberOfEntries=0;

    @Override
    public void initialize(URL fxmlFileLocation, ResourceBundle resources) {

        listView_Titles.setEditable(false);
        listView_Titles.setOrientation(Orientation.VERTICAL);
    }

    private class Entry {
        int handle;
        String title;
        public Entry(int handle, String title) {
            this.handle=handle;
            this.title=title;
        }

        @Override
        public String toString() {
            return title;
        }
    }

    /**
     * Loads the list from an XML document with the form:
     * <PRE>
     *     <objects>
     *         <object>
     *             <attributes>
     *                 <title>title</title>
     *             </attributes>
     *             <tags>
     *                 <tag name="name" value="vale/>...
     *             </tags>
     *         </object>...
     *     </objects>
     * </PRE>
     * @param doc
     * @return
     */
    public boolean loadDocument(Document doc) {
        //Find the objects element
        XPath xp = XPathFactory.newInstance().newXPath();
        ObservableList<Entry> entries = FXCollections.observableArrayList();
        try {
            NodeList nl = (NodeList) xp.compile("//objects/object").evaluate(doc, XPathConstants.NODESET);
            numberOfEntries=nl.getLength();
            for (int x=0; x < numberOfEntries; x++) {
                Element elObject = (Element)(nl.item(x));
                String shandle = xp.compile("attributes/handle/text()").evaluate(elObject);
                int handle = Integer.parseInt(shandle);
                entries.add(new Entry(handle,xp.compile("attributes/title/text()").evaluate(elObject)));
            }
        } catch (Exception e) {
            Util.showError("Parsing Query Results Failed",e.getMessage());
            return false;
        }
        listView_Titles.setItems(entries);
        listView_Titles.getSelectionModel().selectedItemProperty().addListener(

                new ChangeListener<Entry>() {
                    @Override
                    public void changed(ObservableValue<? extends Entry> observable, Entry oldValue, Entry newValue) {
                        saveHandle=newValue.handle;
                        ((Stage)(gridPane_ROOT.getScene().getWindow())).close();
                    }
                });
        return true;
    }

    public void autoShowAndWait(Stage stage) {
        int numberOfEntries=listView_Titles.getItems().size();
        if (numberOfEntries==0)
            return;
        else if (numberOfEntries==1)
            saveHandle=listView_Titles.getItems().get(0).handle;
        else
            stage.showAndWait();
    }

    public int getHandle() {
        return saveHandle;
    }

}
