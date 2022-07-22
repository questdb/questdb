/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.guiutil;

import io.questdb.std.Files;

import javax.swing.*;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.TreePath;
import javax.swing.tree.TreeSelectionModel;
import java.awt.*;
import java.io.File;
import java.util.Arrays;
import java.util.function.Consumer;

class FolderTreePanel extends JPanel {
    private final JTree treeView;
    private final JFileChooser chooser;
    private final Consumer<File> onRootChange;
    private File root;

    FolderTreePanel(Consumer<File> onRootChange, Consumer<TreePath> onSelection) {
        super(new BorderLayout());
        setBorder(BorderFactory.createRaisedBevelBorder());

        this.onRootChange = onRootChange;

        chooser = new JFileChooser();
        chooser.setCurrentDirectory(new java.io.File("."));
        chooser.setDialogTitle("Select a folder");
        chooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
        chooser.setAcceptAllFileFilterUsed(false);
        chooser.setMultiSelectionEnabled(false);

        treeView = new JTree(new DefaultMutableTreeNode("UNDEFINED"));
        treeView.getSelectionModel().setSelectionMode(TreeSelectionModel.SINGLE_TREE_SELECTION);
        treeView.setExpandsSelectedPaths(true);
        treeView.addTreeSelectionListener(e -> onSelection.accept(e.getPath()));
        JScrollPane scrollPane = new JScrollPane();
        scrollPane.getViewport().add(treeView);

        JButton setRootButton = new JButton("Set Root");
        setRootButton.addActionListener(e -> {
            if (FolderTreePanel.this.chooser.showOpenDialog(this) == JFileChooser.APPROVE_OPTION) {
                setRoot(chooser.getSelectedFile());
            }
        });
        JButton refreshButton = new JButton("Reload All");
        refreshButton.addActionListener(e -> reloadModel());
        JPanel buttonPanel = new JPanel();
        buttonPanel.add(setRootButton);
        buttonPanel.add(refreshButton);

        add(buttonPanel, BorderLayout.NORTH);
        add(scrollPane, BorderLayout.CENTER);
    }

    void setRoot(File root) {
        this.root = root;
        reloadModel();
        onRootChange.accept(this.root);
    }

    private void reloadModel() {
        if (root != null) {
            treeView.setModel(createModel(root));
        }
    }

    static String formatItemName(String itemName, long size) {
        if (size < 1024L) {
            return size + " B";
        }
        int z = (63 - Long.numberOfLeadingZeros(size)) / 10;
        return String.format("%s (%.1f %sB)",
                itemName,
                (double) size / (1L << (z * 10)),
                " KMGTPE".charAt(z)
        );
    }

    static String extractItemName(String withSize) {
        if (withSize.endsWith("B)")) {
            return withSize.substring(0, withSize.indexOf(" ("));
        }
        return withSize;
    }

    private static DefaultTreeModel createModel(File folder) {
        return new DefaultTreeModel(addNodes(null, folder));
    }

    private static DefaultMutableTreeNode addNodes(DefaultMutableTreeNode currentRoot, File folder) {
        String folderPath = folder.getPath();
        DefaultMutableTreeNode folderNode = new DefaultMutableTreeNode(folder.getName() + Files.SEPARATOR);
        if (currentRoot != null) { // should only be null at root
            currentRoot.add(folderNode);
        }
        String[] folderContent = folder.list();
        if (folderContent != null && folderContent.length > 0) {
            Arrays.sort(folderContent);
            for (String itemName : folderContent) {
                File newPath = new File(folderPath, itemName);
                if (newPath.isDirectory()) {
                    addNodes(folderNode, newPath);
                } else if (newPath.length() > 0) {
                    folderNode.add(new DefaultMutableTreeNode(formatItemName(itemName, newPath.length())));
                }
            }
        }
        return folderNode;
    }
}
