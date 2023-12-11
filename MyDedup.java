import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class MyDedup {

    public static void upload(int minChunkSize, int avgChunkSize, int maxChunkSize, int base, String filePath){

        private static int mod(int value, int modulus) {
            return value & (modulus - 1);
        }

        private static int modAdd(int a, int b, int modulus) {
            return (mod(a, modulus) + mod(b, modulus)) % modulus;
        }

        private static int modMultiply(int a, int b, int modulus) {
            return (int)(((long)mod(a, modulus) * (long)mod(b, modulus)) % modulus);
        }

        public static int calculateRabinFingerprint(byte[] data, int windowSize) {
            int rfp = 0;
            int dToPowerMMinusOne = 1;
            for (int i = 0; i < windowSize - 1; i++) {
                dToPowerMMinusOne = modMultiply(dToPowerMMinusOne, BASE, MODULUS);
            }

            for (int i = 0; i < windowSize; i++) {
                rfp = modAdd(modMultiply(rfp, BASE, MODULUS), data[i], MODULUS);
            }

            for (int s = 1; s <= data.length - windowSize; s++) {
                rfp = modAdd(modMultiply(BASE, rfp - modMultiply(dToPowerMMinusOne, data[s - 1], MODULUS), MODULUS), data[s + windowSize - 1], MODULUS);
                // Process rfp for each s
            }

            return rfp;
        }

        try{
        Path path = Paths.get(filePath);
        byte[] fileData = Files.readAllBytes(path);
        double fileSize = Files.size(path); // Get file size
        System.out.println("File size: " + fileSize + " bytes");
        } catch (IOException e) {
            e.printStackTrace();
        }

        int rfp = 0;
        for (int i = 0; i < file.length+1; i++){
            if (i == 0){
                for (int j = 0; j < minChunkSize; j++){
                    rfp += fileData[j] * (int) Math.pow(base, minChunkSize-1-i);
                }
                rfp += modMultiply(rfp, avgChunkSize);
            }
            else{
                
            }
        }
    }

    public static void main(String[] args) {
        if (args.length != 6 || !args[0].equals("upload") || !args[0].equals("download")) {
            System.err.println("Usage: java MyDedup upload <min_chunk> <avg_chunk> <max_chunk> <d> <file_to_upload>");
            System.exit(1);
        }

        int minChunkSize = Integer.parseInt(args[1]);
        int avgChunkSize = Integer.parseInt(args[2]);
        int maxChunkSize = Integer.parseInt(args[3]);
        int base = Integer.parseInt(args[4]);
        String filePath = args[5];

    }

    // ... rest of the class
}

