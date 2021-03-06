/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ati.ukwebarchive;

import ati.ukwebarchive.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class extracts textual content from a file
 * @author pierpaolo
 */
public class TestExtraction {

    /**
     * @param args the command line arguments args[0] is the filename
     */
    public static void main(String[] args) {
        try {
            System.out.println(Utils.getContent(new File(args[0])));
        } catch (IOException ex) {
            Logger.getLogger(TestExtraction.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
}
