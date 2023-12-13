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
import java.util.Map;
import java.util.HashMap;
import java.util.*;
import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Scanner;

public class MyDedup {
    static Scanner scanner = new Scanner(System.in);
    private static int anachor (int value, int modulus) {
        return value & (modulus - 1);
    }

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

    public static class Index implements Serializable {
        // public Map<String, List<String>> fileRecipe;
        public Map<String, String> hashIndex;
        public double bytesPreDedup, bytesUnique;
        public int preDedupNum, uniqueNum, fileNum;

        public Index() {
            // fileRecipe = new HashMap<>();
            hashIndex = new HashMap<>();
            bytesPreDedup = 0;
            bytesUnique = 0;
            preDedupNum = 0;
            uniqueNum = 0;
        }

        public boolean isChunkUnique(String hash) {
            return !hashIndex.containsKey(hash);
        }

        public void updateIndex(String hash, String chunkAddress) {
            // if (!fileRecipe.containsKey(filePath)) {
            //     fileRecipe.put(filePath, new ArrayList<>());
            // }
            // fileRecipe.get(filePath).add(hash);

            if (!hashIndex.containsKey(hash)) {
                hashIndex.put(hash, chunkAddress);
            }
        }

        public void saveIndexToFile(String fileName) {
            try {
                File file = new File(fileName);
                if (!file.exists()) {
                    file.createNewFile();  // Create a new file if it doesn't exist
                }
                try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(file))) {
                    oos.writeObject(this);
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }

        public Index readIndexFromFile(String fileName) {
            try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(fileName))) {
                return (Index) ois.readObject();
            } catch (FileNotFoundException e) {
                System.out.println("Index file not found. Creating a new index.");
                Index index = new Index();
                index.saveIndexToFile("MyDedup.index");
                return index;
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
                return null;
            }
        }
    }

    public static class FileRecipe implements Serializable {
        public List<String> hashChunkList;

        public FileRecipe() {
            hashChunkList = new ArrayList<>();
        }

        public void addChunkHash (String hash) {
            hashChunkList.add(hash);
        }

        public void saveFileRecipeToFile(String fileName) {
            try {
                String recipeFileName = "file//" + fileName;
                File recipeFile = new File(recipeFileName);
                if (!recipeFile.exists()) {
                    recipeFile.createNewFile();  // Create a new file if it doesn't exist
                }
                try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(recipeFile))) {
                    oos.writeObject(this);
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }

        public static FileRecipe readFileRecipeFromFile(String fileName) {
            try {
                String recipeFileName = "file//" + fileName;
                File recipeFile = new File(recipeFileName);
                if (!recipeFile.exists()) {
                    System.out.println("File recipe does not exist. Returning a new file recipe.");
                    return new FileRecipe(); // Return a new FileRecipe if the file does not exist
                }
                try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(recipeFile))) {
                    return (FileRecipe) ois.readObject();
                } 
                catch (IOException | ClassNotFoundException e) {
                    e.printStackTrace();
                    return null;
                }
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        }

    }

    public static class Container implements Serializable {
        public static final int MAX_SIZE = 1048576; // 1 MiB
        public ArrayList<Chunk> uniqueChunkList;
        public int currentSize;
        public int containerId;
        // public String containerName;
        // Other attributes as needed, like a map for chunk addresses

        public Container(String fileName) {
            uniqueChunkList = new ArrayList<>();
            currentSize = 0;
            containerId = 1;
            // containerName = fileName;
        }

        public boolean addChunk(Chunk chunk) {
            if (chunk.len + currentSize <= MAX_SIZE) {
                uniqueChunkList.add(chunk);
                currentSize += chunk.len;
                // Update chunk address management
                return true;
            } else {
                // The chunk can't be added due to size limit
                // save current Cotainer to file
                saveContainer();
                containerId++;
                uniqueChunkList = new ArrayList<>();
                currentSize = 0;
                return false;
            }
        }

        // Additional methods like flushing the container to storage, getting chunk addresses, etc.
        public void saveContainer() {
            try {
                // Create directory for this container
                String containerFileName = "data//" + "container" + containerId;
                File containerFile = new File(containerFileName);
                if (!containerFile.exists()) {
                    containerFile.createNewFile(); // Create the directory if it doesn't exist
                }
                ArrayList<byte[]> chunkDataList = new ArrayList<>();
                for (Chunk chunk : uniqueChunkList) {
                    chunkDataList.add(chunk.chunkData);
                }

                try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(containerFile))) {
                    oos.writeObject(chunkDataList);
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
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

    public static String byteArrayToHexString(byte[] bytes) {
        StringBuilder hexString = new StringBuilder();
        for (byte b : bytes) {
            String hex = Integer.toHexString(0xFF & b);
            if (hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }
        return hexString.toString();
    }

    public static void upload(int minChunkSize, int avgChunkSize, int maxChunkSize, int base, String filePath){
        File dataFolder = new File("data/");
        if (!dataFolder.exists()){
            try{
                dataFolder.mkdir();
                System.out.println("Data folder do not exist, created new folder");
            }catch (Exception e){
                e.getStackTrace();
            }
        }
        File recipeFolder = new File("file/");
        if (!recipeFolder.exists()){
            try{
                recipeFolder.mkdir();
                System.out.println("File Recipe folder do not exist, created new folder");
            }catch (Exception e){
                e.getStackTrace();
            }
        }
        byte[] fileData = null;
        File file = new File(filePath);
        String fileName = file.getName();
        int dotIndex = fileName.lastIndexOf('.');
        if (dotIndex > 0) { // Check if there's an extension
            fileName = fileName.substring(0, dotIndex);
        }
        try{
        Path path = Paths.get(filePath);
        fileData = Files.readAllBytes(path);
        // System.out.println("Data sequence: " + fileData[8]);
        // double fileSize = Files.size(path); // Get file size
        // System.out.println("File size: " + fileSize + " bytes");
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        List<Chunk> chunkList = new ArrayList<Chunk>();
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
                // for(int x=0; x< newChunk.chunkData.length ; x++) {
                //     System.out.print(newChunk.chunkData[x] +" ");
                // }
                // System.out.println("Chunk len: " + newChunk.len);
                // System.out.println("===========================");
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
                // for(int x=0; x< newChunk.chunkData.length ; x++) {
                //     System.out.print(newChunk.chunkData[x] +" ");
                // }
                // System.out.println("Chunk len: " + newChunk.len);
                // System.out.println("===========================");
                // i += minChunkSize;
                continue;
            }
            // Calculate RFP
            if (i == 0){
                for (int j = 0; j < minChunkSize; j++){
                    rfp += fileData[j] * (int) Math.pow(base, minChunkSize-1-j);
                    // System.out.println("Summation: " + rfp);
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
            if ( anachor(rfp, avgChunkSize) == 0 ) {
                currentAnchor = i + minChunkSize - 1;
                // System.out.println("currentAnchor: " + currentAnchor);
                byte[] chunkData = Arrays.copyOfRange(fileData, lastAnchor+1, currentAnchor + 1);
                Chunk newChunk = new Chunk(chunkData, currentAnchor - lastAnchor);
                newChunk.startIndex = lastAnchor + 1;
                newChunk.endIndex = currentAnchor;
                chunkList.add(newChunk);
                lastAnchor = currentAnchor;
                System.out.println("Chunked, starting index = " + newChunk.startIndex);
                // for(int x=0; x< newChunk.chunkData.length ; x++) {
                //     System.out.print(newChunk.chunkData[x] +" ");
                // }
                // System.out.println("Chunk len: " + newChunk.len);
                // System.out.println("===========================");
                // i += minChunkSize;
            }
            // scanner.nextLine();
        }

        Container procContainer = new Container(fileName);
        Index index = new Index();
        FileRecipe procFileRecipe = new FileRecipe();
        index = index.readIndexFromFile("MyDedup.index");
        double bytesPreDedup = 0;
        double bytesUnique = 0;
        int uniqueNum = 0;
        for (int i = 0; i < chunkList.size() ; i++) {
            // System.out.println("Chunk length = " + chunkList.get(i).len);
            Chunk procChunk = chunkList.get(i);
            System.out.println("Current Chunk Staring Index = " + procChunk.startIndex);
            bytesPreDedup += procChunk.len;
            try {
                MessageDigest md = MessageDigest.getInstance("SHA-1");
                md.update(procChunk.chunkData, 0, procChunk.len);
                byte[] checkSumBytes = md.digest();
                procChunk.hashVal = byteArrayToHexString(checkSumBytes);
                // System.out.println("checkSumStr: " + procChunk.hashVal);
                procFileRecipe.addChunkHash(procChunk.hashVal);
                if (index.isChunkUnique(procChunk.hashVal)) {
                    // System.out.println("procContainer.uniqueChunkList Size = " + procContainer.uniqueChunkList.size());
                    // index.updateIndex(procChunk.hashVal, procContainer.containerId + "_" + procContainer.uniqueChunkList.size());
                    bytesUnique += procChunk.len;
                    uniqueNum++;
                    // add to Container
                    boolean isChunkAdded = procContainer.addChunk(procChunk);
                    if (isChunkAdded) {
                        index.updateIndex(procChunk.hashVal, procContainer.containerId + "_" + (procContainer.uniqueChunkList.size() - 1));
                        // Rest of your logic
                    } else {
                        // Handle the case where chunk will be added to the next container
                        procContainer.addChunk(procChunk);
                        index.updateIndex(procChunk.hashVal, procContainer.containerId + "_" + (procContainer.uniqueChunkList.size() - 1));
                    }
                }
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }
        procFileRecipe.saveFileRecipeToFile(fileName);
        // System.out.println("===========================");
        index.preDedupNum += chunkList.size();
        index.uniqueNum += uniqueNum;
        index.bytesPreDedup += bytesPreDedup;
        index.bytesUnique += bytesUnique;
        index.fileNum++;
        // System.out.println("preDedupNum: " + index.preDedupNum);
        // System.out.println("uniqueNum: " + index.uniqueNum);
        // System.out.println("bytesPreDedup: " + index.bytesPreDedup);
        // System.out.println("bytesUnique: " + index.bytesUnique);
        index.saveIndexToFile("MyDedup.index");
        if (procContainer.currentSize < procContainer.MAX_SIZE && procContainer.currentSize != 0) {
            procContainer.saveContainer();
        }

        Index result = new Index();
        result = result.readIndexFromFile("MyDedup.index");
        double deDupRatio = 0;
        // if (bytesUnique == 0) {
        //     deDupRatio = 0;
        // } else {
            deDupRatio = result.bytesPreDedup/result.bytesUnique;
        // }
        //System.out.println("filePath: " + (fileName + "container" + procContainer.containerId));
        // System.out.println("===========================");
        System.out.println("Report Output:");
        System.out.println("Total number of files that have been stored: " + result.fileNum);
        System.out.println("Total number of pre-deduplicated chunks in storage: " + result.preDedupNum);
        System.out.println("Total number of unique chunks in storage: " + result.uniqueNum);
        System.out.println("Total number of bytes of pre-deduplicated chunks in storage: " + result.bytesPreDedup);
        System.out.println("Total number of bytes of unique chunks in storage: "+ result.bytesUnique);
        System.out.println("Total number of containers in storage: " + procContainer.containerId);
        System.out.printf("Deduplication ratio: %.2f\n", deDupRatio);
        System.out.println("Upload Complete");
    }

    public static void download(Index index, String fileToDownload, String localFileName) {
        String fileNameWithoutExtension = fileToDownload.substring(0, fileToDownload.lastIndexOf('.'));
        String path = "data//";
        FileRecipe fileRecipe = FileRecipe.readFileRecipeFromFile(fileNameWithoutExtension);
        Map<Integer, ArrayList<byte[]>> containerCache = new HashMap<>();
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();

        try {
            for (String hash : fileRecipe.hashChunkList) {
                String hashValue = index.hashIndex.get(hash);
                String[] parts = hashValue.split("_");
                if (parts.length == 2) {
                    int containerId = Integer.parseInt(parts[0]);
                    int listId = Integer.parseInt(parts[1]);

                    if (!containerCache.containsKey(containerId)) {
                        try (ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(new FileInputStream(path + "container" + containerId)))) {
                            @SuppressWarnings("unchecked")
                            ArrayList<byte[]> containerData = (ArrayList<byte[]>) ois.readObject();
                            containerCache.put(containerId, containerData);
                        } catch (IOException | ClassNotFoundException e) {
                            e.printStackTrace();
                        }
                    }

                    ArrayList<byte[]> cachedContainerData = containerCache.get(containerId);
                    if (cachedContainerData != null) {
                        byte[] chunk = cachedContainerData.get(listId);
                        byteStream.write(chunk);
                    }
                }
            }

            // Writing the accumulated bytes to the output file in one go
            try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(localFileName))) {
                byteStream.writeTo(bos);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Download Complete");
    }

    public static void main(String[] args) {
        if (args.length != 6 && args.length != 3) {
            System.err.println("Usage: java MyDedup upload <min_chunk> <avg_chunk> <max_chunk> <d> <file_to_upload>");
            System.err.println("Usage: java MyDedup download <file_to_download> <local_file_name>");
            System.exit(1);
        }

        if (args[0].equals("upload")) {
            System.out.println("Uploading");
            int minChunkSize = Integer.parseInt(args[1]);
            int avgChunkSize = Integer.parseInt(args[2]);
            int maxChunkSize = Integer.parseInt(args[3]);
            int base = Integer.parseInt(args[4]);
            String filePath = args[5];
            upload(minChunkSize, avgChunkSize, maxChunkSize, base, filePath);
            // System.out.println("filePath: " + filePath);
        } else if (args[0].equals("download")) {
            Index index = new Index();
            index = index.readIndexFromFile("MyDedup.index");
            System.out.println("Downloading");
            // System.out.println("chunks: " + index.chunks.get("83a002e8ffbe10a8e5bfd289b565b247092a9b70")[0]);
            String fileToDownload = args[1];
            String localFileName = args[2];
            download(index, fileToDownload, localFileName);
        } else {
            System.err.println("Usage: java MyDedup upload <min_chunk> <avg_chunk> <max_chunk> <d> <file_to_upload>");
            System.err.println("Usage: java MyDedup download <file_to_download> <local_file_name>");
            System.exit(1);
        }
        // String content = "19648635";
        // byte[] fileData = content.getBytes();

        // Path path = Paths.get("output.txt");
        // try {
        //     Files.write(path, fileData);
        //     System.out.println("File created successfully.");
        // } catch (IOException e) {
        //     e.printStackTrace();
        // }

    }

    // ... rest of the class
}

