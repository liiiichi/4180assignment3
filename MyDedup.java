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
    private static class Chunk {
        byte[] chunkData;
        int startIndex = 0;
        int endIndex = 0;
        int len = 0;
        String hashVal = "";

        public Chunk(byte[] data, int len) {
            this.chunkData = data;
            this.len = len;
        }

        public Chunk(){
            this.chunkData = new byte[0];
        }
    }

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
        
        List<Chunk> chunkList = new ArrayList<Chunk>();
        // Chunk procChunk = new Chunk();
        int rfp = 0;
        int lastAnchor = -1;
        int currentAnchor = 0;
        int checkAnchor;
        for (int i = 0; i < fileData.length+1; i++){
            // Single Chunk can store the whole file
            // if (maxChunkSize >= fileData.length){
            //     chunkList.add(new Chunk(fileData, fileData.length));
            //     break;
            // }

            // EOF
            if (i + minChunkSize >= fileData.length) {
                currentAnchor = fileData.length - 1;
                byte[] chunkData = Arrays.copyOfRange(fileData, lastAnchor+1, currentAnchor + 1);
                Chunk newChunk = new Chunk(chunkData, currentAnchor - lastAnchor);
                newChunk.startIndex = lastAnchor + 1;
                newChunk.endIndex = currentAnchor;
                chunkList.add(newChunk);
                lastAnchor = currentAnchor;
                for(int x=0; x< newChunk.chunkData.length ; x++) {
                    System.out.print(newChunk.chunkData[x] +" ");
                }
                System.out.println("Chunk len: " + newChunk.len);
                System.out.println("===========================");
                break;
            }
            // before next Anchor Point, reach maxChunkSize
            if (((checkAnchor = i + minChunkSize - 1) - lastAnchor) == maxChunkSize) {
                currentAnchor = checkAnchor;
                byte[] chunkData = Arrays.copyOfRange(fileData, lastAnchor+1, currentAnchor + 1);
                Chunk newChunk = new Chunk(chunkData, currentAnchor - lastAnchor);
                newChunk.startIndex = lastAnchor + 1;
                newChunk.endIndex = currentAnchor;
                chunkList.add(newChunk);
                lastAnchor = currentAnchor;
                for(int x=0; x< newChunk.chunkData.length ; x++) {
                    System.out.print(newChunk.chunkData[x] +" ");
                }
                System.out.println("Chunk len: " + newChunk.len);
                System.out.println("===========================");
                i += minChunkSize;
                continue;
            }
            // Calculate RFP
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
            if (rfp == 0) {
                currentAnchor = i + minChunkSize - 1;
                System.out.println("currentAnchor: " + currentAnchor);
                byte[] chunkData = Arrays.copyOfRange(fileData, lastAnchor+1, currentAnchor + 1);
                Chunk newChunk = new Chunk(chunkData, currentAnchor - lastAnchor);
                newChunk.startIndex = lastAnchor + 1;
                newChunk.endIndex = currentAnchor;
                chunkList.add(newChunk);
                lastAnchor = currentAnchor;
                for(int x=0; x< newChunk.chunkData.length ; x++) {
                    System.out.print(newChunk.chunkData[x] +" ");
                }
                System.out.println("Chunk len: " + newChunk.len);
                System.out.println("===========================");
                i += minChunkSize;
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

