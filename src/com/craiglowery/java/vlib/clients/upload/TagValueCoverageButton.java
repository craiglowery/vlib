package com.craiglowery.java.vlib.clients.upload;

import javafx.scene.control.Button;
import javafx.scene.layout.Background;
import javafx.scene.layout.BackgroundFill;
import javafx.scene.paint.Paint;

/**
 * Created by Craig on 3/4/2016.
 */
public class TagValueCoverageButton extends Button {

    public enum Coverage { ALL, SOME, NONE };
    private Coverage coverage;

    public TagValueCoverageButton(String value, Coverage coverage) {
        super();
        setText(value);
        setCoverage(coverage);
        setOnAction(
                event -> {
                    cycleCoverage();
                }
        );
    }

    public void setCoverage(Coverage coverage) {
        String color="grey";
        switch (coverage) {
            case ALL: color="green"; break;
            case SOME: color="yellow"; break;
        }
        setBackground(new Background(new BackgroundFill(Paint.valueOf(color),null,null)));
        this.coverage=coverage;
    }

    public Coverage getCoverage() {
        return coverage;
    }

    public void cycleCoverage() {
        switch (coverage) {
            case ALL:
                setCoverage(Coverage.NONE);
                break;
            case NONE:
            case SOME:
                setCoverage(Coverage.ALL);
                break;
        }
    }

}
