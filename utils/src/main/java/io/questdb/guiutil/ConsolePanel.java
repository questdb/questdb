/* **
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Copyright (c) 2019 - 2022, Miguel Arregui a.k.a. marregui
 */

package io.questdb.guiutil;

import javax.swing.*;
import java.awt.*;


class ConsolePanel extends JPanel {

    private static final long serialVersionUID = 1L;
    private static final Font FONT = new Font("Monospaced", Font.BOLD, 14);
    private static final Color FONT_COLOR = new Color(200, 50, 100);

    private final JTextPane textPane;

    ConsolePanel() {
        super(new BorderLayout());
        textPane = new JTextPane();
        FontMetrics metrics = textPane.getFontMetrics(FONT);
        int vMargin = metrics.getHeight();
        int hMargin = metrics.stringWidth("####");
        Insets margin = new Insets(vMargin, hMargin, vMargin, hMargin);
        textPane.setMargin(margin);
        textPane.setFont(FONT);
        textPane.setForeground(FONT_COLOR);
        textPane.setBackground(Color.BLACK);
        textPane.setEditable(false);
        add(new JScrollPane(textPane), BorderLayout.CENTER);
    }

    void display(String message) {
        textPane.setText(message);
        repaint();
    }
}
