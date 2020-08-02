package com.craiglowery.java.vlib.clients.server.connector;

import com.craiglowery.java.common.Util;
import javafx.collections.ObservableList;
import javafx.fxml.FXMLLoader;
import javafx.fxml.Initializable;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.ChoiceBox;
import javafx.scene.layout.BorderPane;
import javafx.stage.Stage;

import java.net.URL;
import java.util.ResourceBundle;

/**
 * Created by craig on 12/3/2016.
 */
public class ConnectionChooserController implements Initializable {
    public ChoiceBox<Profile> cbProfile;
    public Button btOk;

    public class Profile {
        public String name;
        public String baseUrl;
        public String username;
        public String password;

        public Profile(String name, String baseUrl, String username, String password) {
            this.name=name;
            this.baseUrl=baseUrl;
            this.password=password;
            this.username=username;
        }

        @Override
        public String toString() {
            return name;
        }
    }



    @Override
    public void initialize(URL fxmlFileLocation, ResourceBundle resources) {
        ObservableList<Profile> profiles = cbProfile.getItems();
        profiles.add(new Profile(
                "Google Audio",
                "http://vlib-server.craiglowery.com:8080/vlib/api",
                "editor",
                "OlagGan2000"));
        profiles.add(new Profile(
                "Local Video",
                "http://192.168.1.14:8080/vlib/api",
                "vlibrarian",
                "OlagGan2000"
        ));
        btOk.setOnAction(event -> { btOk.getScene().getWindow().hide(); } );
    }

    public static Profile chooseProfile() {
        Profile profile = null;
        try {
            URL fxml = ConnectionChooserController.class.getResource("ConnectionChooser.fxml");
            FXMLLoader fxmlLoader = new FXMLLoader(fxml);
            BorderPane pane = fxmlLoader.load();
            ConnectionChooserController controller = fxmlLoader.getController();
            Scene scene = new Scene(pane);
            Stage stage = new Stage();
            stage.setScene(scene);
            stage.setTitle("Server Profile Chooser");
           // stage.showAndWait();
            //profile = controller.cbProfile.getSelectionModel().getSelectedItem();
            profile = controller.cbProfile.getItems().get(1);
            stage.hide();
        } catch (Exception e) {
            Util.showError("Error displaying server profiles",e.getMessage());
        }
        return profile;
    }
}