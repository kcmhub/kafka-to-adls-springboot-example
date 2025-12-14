package io.kcm.kafka.adls.services;

import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakePathClientBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

@Service
public class AdlsWriterService {

    private final String accountName;
    private final String filesystem;
    private final String basePath;
    private final String sasToken;

    public AdlsWriterService(
            @Value("${app.adls.account-name}") String accountName,
            @Value("${app.adls.filesystem}") String filesystem,
            @Value("${app.adls.base-path}") String basePath,
            @Value("${app.adls.sas-token}") String sasToken
    ) {
        this.accountName = accountName;
        this.filesystem = filesystem;
        this.basePath = basePath;
        this.sasToken = cleanupSasToken(sasToken);
    }

    private String cleanupSasToken(String raw) {
        if (raw == null) {
            return null;
        }
        // Si jamais tu l'as mis avec un "?" devant, on le retire
        return raw.startsWith("?") ? raw.substring(1) : raw;
    }

    private DataLakeFileClient getFileClient(String filePath) {
        // Endpoint ADLS Gen2 en dfs (et non blob)
        String dfsEndpoint = String.format(
                "https://%s.dfs.core.windows.net",
                accountName
        );

        return new DataLakePathClientBuilder()
                .endpoint(dfsEndpoint)
                .fileSystemName(filesystem)
                .pathName(filePath)
                .sasToken(sasToken)
                .buildFileClient();
    }

    /**
     * Crée un fichier unique et écrit tout le contenu dedans
     */
    public void writeNewFile(String topic,
                             Integer partition,
                             Long startOffset,
                             String content) {
        String date = LocalDate.now().format(DateTimeFormatter.BASIC_ISO_DATE);

        String fileName = String.format(
                "%s-%d-start_offset=%d.log",
                topic,
                partition,
                startOffset
        );

        String filePath = String.format("%s/date=%s/%s", basePath, date, fileName);

        DataLakeFileClient fileClient = getFileClient(filePath);
        fileClient.create(true); // overwrite=true mais fichier n'existe pas → ok

        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);

        // append
        fileClient.append(inputStream, 0, bytes.length);

        // flush sécurisée : overwrite = true pour éviter les 412
        fileClient.flush(bytes.length, true);
    }
}


