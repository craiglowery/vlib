package com.craiglowery.java.vlib.clients.upload;

import javafx.collections.ObservableList;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;

import java.util.*;
import java.util.prefs.BackingStoreException;
import java.util.prefs.Preferences;

/**
 * <p>Used to persist table column display information:</p>
 * <ul>
 *     <li>Name</li>
 *     <li>Column Position</li>
 *     <li>Sort Direction</li>
 *     <li>Width</li>
 *     <li>Position in sort order</li>
 * </ul>
 *
 * <p>Profiles can be created either by using a TableView object as a pattern,
 *    or by loading the profile from the backing store using its name as a key.</p>
 *
 * Created by Craig on 3/7/2016.
 */
public class ViewProfile<VIEW_ITEM_TYPE>  {

    /** The key for the node under which all profiles will be stored (profile collection node) **/
    public static final String COLUMN_ORDERINGS_KEY = "columnOrderings";

    /** The node under which the collection of profiles is stored **/
    private Preferences profileCollectionNode;

    /** The preference node under the profile collection node where this profile's data is stored **/
    private Preferences myNode;

    /** The name of this profile **/
    private String name=null;

    /** The in-memory key/value mapping of column info objects for each column **/
    private Map<String,ColumnInfo> info;

    /** A convenience/performance mapping that contains the ColumnInfo objrects designated as sortOrder participants **/
    private SortedMap<Integer,ColumnInfo> sortOrderMap = new TreeMap<>();

    /** Returns the preference node for the entire package.  The preference node that contains
     * the profile collection should be a direct descendant of this node.
     * @return The preference node for the package.
     * @throws BackingStoreException
     */
    private static Preferences getRootPreferencesNode() throws BackingStoreException {
        return Preferences.userNodeForPackage(ViewProfile.class);
    }

    /** Returns the preference node which contains the nodes of each individual profile's preferences.
     *  The child nodes are named with the name of the profile whose settings they contain.
     * @return The preference node for the profile collection.
     * @throws BackingStoreException
     */
    private static Preferences getProfileCollectionNode() throws BackingStoreException{
        return getRootPreferencesNode().node(COLUMN_ORDERINGS_KEY);
    }

    /**
     * Creates and returns a profile preference node for a named profile.
     * @param name The name of the profile to create/retrieve.
     * @return The preference node for the named profile.
     * @throws BackingStoreException
     */
    private static Preferences getMyNode(String name) throws BackingStoreException {
        return getProfileCollectionNode().node(name);
    }

    /**
     * Retrieves the profile preference node for this profile.
     * @return The preference node for this profile.
     * @throws BackingStoreException
     */
    private Preferences getMyNode() throws BackingStoreException {
        return getMyNode(name);
    }

    /**
     * <p>Constructs a new profile object of the given name as follows.</p>
     *
     * <ol>
     *      <li>If the name is null or the empty string, it is replaced with the name "default".</li>
     *      <li>If the profile exists in the backing store, the settings are retrieved and applied
     *         additively to what was initialized from the {@code TableView}.
     * </ol>
     * @param name The name of the profile to be retrieved or created.
     * @throws BackingStoreException
     */
    public ViewProfile(String name) throws BackingStoreException {
        //Basic initialization for an "empty" profile
        if (name==null || name.trim().equals(""))
            name="default";
        this.name=name;
        profileCollectionNode = getProfileCollectionNode();
        info=new HashMap<>();
        myNode = getMyNode();

        //Load any existing column info
        for (String key : myNode.keys())
            if (key.startsWith("COLUMN_")){
                ColumnInfo columnInfo = parseColumnInfo(myNode.get(key, null));
                if (columnInfo != null)
                    info.put(columnInfo.name, columnInfo);
            }
        computeSortOrderMap();
    }

    /**
     * <p>Updates the profile to incorporate aspects of the provided view as follows:</p>
     *
     * <ol>
     *     <li>If a view column is already in the profile, the profile is updated to reflect its characteristics
     *     in the view.</li>
     *     <li>If a view column is not in the profile, it is added to the profile.</li>
     *     <li>The ordering of columns in the profile will include the view columns in the same order as they
     *     appear in the view, followed by any old columns in the order the existed prior to the update.</li>\
     *     <li>The updated profile is persisted.</li>
     * </ol>
     *
     * @param view The view from which to update the profile.
     */
    public void update(TableView<VIEW_ITEM_TYPE> view) throws BackingStoreException {

        //Go through the profile's existing column info objects and shift the index really high, to preserve their order
        //Set the sort order to "not included"
        for (ColumnInfo columnInfo : info.values()) {
            columnInfo.index += 1000;
            columnInfo.sortOrder=0;
        }

        Map<TableColumn<VIEW_ITEM_TYPE,?>,ColumnInfo> quickMap = new HashMap<>();

        //Scan the table columns and store/update their info
        for (TableColumn<VIEW_ITEM_TYPE, ?> column : view.getColumns()) {
            ColumnInfo columnInfo = new ColumnInfo(column);
            info.put(columnInfo.name,columnInfo);
            quickMap.put(column,columnInfo);
        }

        //Set the sort order values
        int x=1;
        for (TableColumn<VIEW_ITEM_TYPE,?> column : view.getSortOrder()) {
            quickMap.get(column).sortOrder=x++;
        }
        computeSortOrderMap();

        //Update the indexes for the "unused" columns in the profile so they are contiguous
        normalizeColumnInfoIndexes();

        persist();
    }

    /** Scans the column info {@code info} list and populates the {@code sortOrderMap} with references
     * to those having a non-zero non-negative sortOrder value.
     */
    private void computeSortOrderMap() {
        sortOrderMap.clear();
        for (ColumnInfo columnInfo : info.values())
            if (columnInfo.sortOrder>0)
                sortOrderMap.put(columnInfo.sortOrder,columnInfo);
    }

    /** Normalizes the column info "index" properties, keeping them in relative order, but making them
     * a sequence starting at 0 and incrementing.
     */
    private void normalizeColumnInfoIndexes() {
        //Make a map that puts the column info objects in order by index
        TreeMap<Integer,ColumnInfo> sorted = new TreeMap<>();
        for (ColumnInfo columnInfo : info.values())
            sorted.put(columnInfo.index,columnInfo);
        //Walk the tree in order and update the indexes to to be a contiguous sequence
        // of non-negative integers starting with 0
        int x=0;
        for (ColumnInfo columnInfo : sorted.values())
            columnInfo.index=x++;
    }

    /** Completely persists the profile to the backing store, removing any previous version of this profile by name. **/
    private void persist() throws BackingStoreException {
        myNode.removeNode();
        myNode = getMyNode();
        for (ColumnInfo columInfo : info.values()) {
            columInfo.persist();
        }
        myNode.flush();
    }

    /**
     * Determines if there is an ordering (profile) with the provided name.
     * @param name Name of interest.
     * @return True if an ordering (profile) by that name exists.
     * @throws BackingStoreException
     */
    public static boolean profileExists(String name) throws BackingStoreException {
        return getProfileCollectionNode().nodeExists(name);
    }

    /**
     * <p>Applies this profile to a view as follows:</p>
     *
     * <ol>
     *     <li>All columns that exist in both the profile and the view are collected, ordered according to the
     *     indexes in the profile column info database, and have their sort types and widths set to the profile's
     *     values.</li>
     *     <li>Columns that remain in the view are appended to the right of those collected above.</li>
     *     <li>The profile's sort order is finally applied. If a column name is in the profile's sort order
     *     that is not in the view, then it is ignored (as if it were not in the list).</li>
     * </ol>
     *
     * @param view
     */
    public void apply(TableView<VIEW_ITEM_TYPE> view) {
        // viewColumns is a shortcut to the actual observable list in the TableView
        // buildColumns is where we are building the new list to replace it, sorted by index
        // holdList is the copy of the viewColumns list that we will remove things from
        // nameMap is a mapping to column objects by name
        // sortMap is a sorted mapping of ColumnInfo by sort order
        ObservableList<TableColumn<VIEW_ITEM_TYPE,?>>  viewColumns = view.getColumns();
        SortedMap<Integer,TableColumn<VIEW_ITEM_TYPE,?>> buildColumns = new TreeMap<>();
        List<TableColumn<VIEW_ITEM_TYPE,?>> holdList = new LinkedList<>(viewColumns);
        Map<String,TableColumn<VIEW_ITEM_TYPE,?>> nameMap = new HashMap<>();

        //Iterate over the columns in the view, building the nameMap while also looking for those for
        //which we have a ColumnInfo object.
        for (TableColumn<VIEW_ITEM_TYPE,?> column : viewColumns) {
            String columnName = column.getText();
            nameMap.put(columnName,column);
            ColumnInfo columnInfo = info.get(columnName);
            if (columnInfo!=null) {
                //If we have a matching ColumnInfo object, we remove it from the holdList and place it
                //into the buildList sorted to the position based on its index.  We also set the width and
                //sortType to match what the profile requires.
                holdList.remove(column);
                column.setPrefWidth(columnInfo.width);
                column.setSortType(columnInfo.sortType);
                buildColumns.put(columnInfo.index,column);
            }
        }
        //If there are any columns left over the the holdList, append them to the end of the buildList
        for (TableColumn<VIEW_ITEM_TYPE,?> column : holdList)
            buildColumns.put(1000+buildColumns.size(),column);

        //Remove all columns from the view, then add them back in the order specified by the
        //sorted buildList.
        viewColumns.clear();
        for (TableColumn<VIEW_ITEM_TYPE,?> column : buildColumns.values())
            viewColumns.add(column);

        //Apply the sort order
        ObservableList<TableColumn<VIEW_ITEM_TYPE,?>> sortColumns = view.getSortOrder();
        sortColumns.clear();  //Clear any existing sort order in the view
        for (ColumnInfo columnInfo : sortOrderMap.values())
            if (nameMap.containsKey(columnInfo.name))
                sortColumns.add(nameMap.get(columnInfo.name));
    }

    /**
     * Deletes this profile from the backing store.  This profile object is still available to be applied to
     * views, but will not be persisted unless {@code update()} is subsequently called.
     */
    public void delete() throws BackingStoreException {
        if (profileExists(name))
             profileCollectionNode.node(name).removeNode();
    }

    /**
     * Removes all profiles from the backing store.
     */
    public static void purge() throws BackingStoreException {
        Preferences root = getRootPreferencesNode();
        if (root.nodeExists(COLUMN_ORDERINGS_KEY))
            root.node(COLUMN_ORDERINGS_KEY).removeNode();
    }

    /**
     * Creates a column info object from a string representation of it.
     * @param s An encoded representation of the object.
     * @return The reconstituted object.
     */
    public ColumnInfo parseColumnInfo(String s) {
        if (s==null) return null;
        String[] fields = s.split(",");
        if (fields.length!=5) return null;
        try {
            if (fields[0].trim().equals("")) return null;
            int index=Integer.parseInt(fields[1]);
            double width=Double.parseDouble(fields[2]);
            int sortType_i = Integer.parseInt(fields[3]);
            int sortOrder = Integer.parseInt(fields[4]);
            return new ColumnInfo(
                    fields[0],
                    index,
                    width,
                    sortType_i==0?TableColumn.SortType.ASCENDING:TableColumn.SortType.DESCENDING,
                    sortOrder);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    //-----------------------------------------------------------------------------------------------------

    /**
     * A private inner class that represents the column settings we care about.
     */
    private class ColumnInfo {
        String name;
        int index;
        double width;
        TableColumn.SortType sortType;
        int sortOrder;   //<=0 not in the sort order, otherwise order by positive integer

        /**
         * CONSTRUCTOR.
         * Creates a ColumnInfo from scratch using explicit values.
         * @param name The name of the column.
         * @param index The relative position in the column display order, left-to-right.
         * @param width The width in pixels.
         * @param sortType ASCENDING or DESCENDING.
         */
        public ColumnInfo(String name, int index, double width, TableColumn.SortType sortType,int sortOrder) {
            this.name=name;
            this.index=index;
            this.width=width;
            this.sortType=sortType;
            this.sortOrder=sortOrder;
        }

        /**
         * CONSTRUCTOR.
         * Creates a ColumnInfo by examining a TableColumn.
         * @param column The column to be examined and characterized.
         */
        public ColumnInfo(TableColumn<VIEW_ITEM_TYPE,?> column) {
            name=column.getText();
            index=column.getTableView().getColumns().indexOf(column);
            width=column.getWidth();
            sortType=column.getSortType();
        }

        /**
         * Returns a string encoding of the object, suitable for storing in a preferences backing store.
         * @return
         */
        @Override
        public String toString() {
            return String.format("%s,%d,%f,%d,%d",name,index,width,sortType==TableColumn.SortType.ASCENDING?0:1,sortOrder);
        }

        /**
         * Persists the key/value pair for this ColumnInfo in the backing store.
         */
        void persist() {
            if (myNode!=null) {
                myNode.put( "COLUMN_"+name,toString());
            }
        }
    }

}
