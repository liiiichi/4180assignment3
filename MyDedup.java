import java.io.IOException;
import java.io.ObjectInput;
import java.io.OutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.math.BigInteger;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.io.File;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.*;
import java.nio.file.Path;
import java.nio.file.Paths;

public class MyDedup {

    // private static int mod(int value, int modulus) {
    //     return value & (modulus - 1);
    // }

    private static int mod(int value, int modulus) {
        return ((value % modulus) + modulus) % modulus;
    }

    private static int modAdd(int a, int b, int modulus) {
        return (mod(a, modulus) + mod(b, modulus)) % modulus;
    }

    private static int modMultiply(int a, int b, int modulus) {
        return (int)(((long)mod(a, modulus) * (long)mod(b, modulus)) % modulus);
    }

    public static void upload(int minChunkSize, int avgChunkSize, int maxChunkSize, int base, String filePath){
        byte[] fileData = null;
        try{
        Path path = Paths.get(filePath);
        fileData = Files.readAllBytes(path);
        // System.out.println("Data sequence: " + fileData[8]);
        double fileSize = Files.size(path); // Get file size
        System.out.println("File size: " + fileSize + " bytes");
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        int rfp = 0;
        for (int i = 0; i < fileData.length+1; i++){
            if (i == 0){
                for (int j = 0; j < minChunkSize; j++){
                    rfp += fileData[j] * (int) Math.pow(base, minChunkSize-1-j);
                    System.out.println("Summation: " + rfp);
                }
                rfp = mod(rfp, avgChunkSize);
                System.out.println("first rfp = " + rfp);
            }
            else {
                int tmp = rfp;
                int baseTsModMult = modMultiply(fileData[i], (int) Math.pow(base, minChunkSize - 1), avgChunkSize);
                int secPartMod = tmp - baseTsModMult;
                int dSecModMult = modMultiply(base, secPartMod, avgChunkSize);
                rfp = modAdd(dSecModMult, fileData[i + minChunkSize - 1], avgChunkSize);
                // rfp = mod((base * (tmp - baseTsModMult) + fileData[i + minChunkSize - 1]), avgChunkSize);
                 System.out.println("next rfp = " + rfp);
            }
        }
    }

    public static void main(String[] args) {
        if (args.length != 6) {
            System.err.println("Usage: java MyDedup upload <min_chunk> <avg_chunk> <max_chunk> <d> <file_to_upload>" + args.length);
            System.exit(1);
        }
        System.out.println("Start Upload");
        int minChunkSize = Integer.parseInt(args[1]);
        int avgChunkSize = Integer.parseInt(args[2]);
        int maxChunkSize = Integer.parseInt(args[3]);
        int base = Integer.parseInt(args[4]);
        String filePath = args[5];

        // String content = "19648635";
        // byte[] fileData = content.getBytes();

        // Path path = Paths.get("output.txt");
        // try {
        //     Files.write(path, fileData);
        //     System.out.println("File created successfully.");
        // } catch (IOException e) {
        //     e.printStackTrace();
        // }

        upload(minChunkSize, avgChunkSize, maxChunkSize, base, filePath);
    }

    // ... rest of the class
}

