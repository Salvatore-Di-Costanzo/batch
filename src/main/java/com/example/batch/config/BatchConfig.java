package com.example.batch.config;

import com.example.batch.DAO.UserDAO;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.*;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.PathResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;

import java.io.File;

@Configuration
@EnableBatchProcessing
public class BatchConfig {


    // Job builder factory serve per creare un Job, nel metodo get andiamo a specificare quale è il nome del Job ( cioè della funzione ) da richiamare
    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    // Step builder factory serve per andare in qualche modo a specificare quali sono gli step ( cioè funzioni ) da richiamare
    // per ndare a dichiarare uno step oltre alla step build factory che effettivamente ritorna l'oggetto il metodo deve ritornare un tipo Step es:
    // public Step step1 () {
    //  return stepbuildfactory.get("step1")
    //          .<
    //
    //
    // }
    @Autowired
    private StepBuilderFactory stepBuilderFactory;



    @Bean
    public TaskExecutor taskExecutor(){
        SimpleAsyncTaskExecutor simpleAsyncTaskExecutor = new SimpleAsyncTaskExecutor();
        simpleAsyncTaskExecutor.setConcurrencyLimit(5);
        return simpleAsyncTaskExecutor;
    }

    @Bean
    public Job job() {
        return jobBuilderFactory.get("job")
                .start(splitFlow())
                .build()        //Build dell'istanza FlowBuilder
                .build();       //Build dell'istanza Job
    }

    @Bean
    public Job ReadWrite() {
        return jobBuilderFactory.get("job1")
                .start(splitFlow())
                .build()
                .build();
    }




    @Bean
    public FlatFileItemReader<UserDAO> itemReader0() {

        // Impostiamo i field di destinazione derivandoli direttamente dalla classe UserDAO
        BeanWrapperFieldSetMapper<UserDAO> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(UserDAO.class);

        // Impostiamo il delimitatore come "," e andiamo ad impostare i nomi delle "colonne" del csv
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter(",");
        lineTokenizer.setNames(new String[] {"firstname", "dob"});

        // Andiamo ad impostare nel line mapper i valori dei field ( relativi alla classe) e alla tokenizzazione ( relativa alle righe del CSV )
        DefaultLineMapper<UserDAO> lineMapper = new DefaultLineMapper<>();
        lineMapper.setFieldSetMapper(fieldSetMapper);
        lineMapper.setLineTokenizer(lineTokenizer);

        // Impostiamo infine il file di riferimento dove andare ad effettuare la lettura impostando anche il line mapper
        FlatFileItemReader<UserDAO> flatFileItemReader = new FlatFileItemReader<>();
        flatFileItemReader.setName("userItemReader");
        flatFileItemReader.setResource(new PathResource("C:\\Users\\sdicostanzo\\Downloads\\myFile0.csv"));
        flatFileItemReader.setLineMapper(lineMapper);
        flatFileItemReader.setLinesToSkip(1);

        return flatFileItemReader;
    }

    @Bean
    public FlatFileItemWriter<UserDAO> itemWriter0() {

        // Questo oggetto è un estrattore di campo per un Java Beans. Dato un array di nomi di proprietà
        // chiamerà in modo riflessivo i getter sull'elemento e restituirà un array di tutti i valori
        BeanWrapperFieldExtractor<UserDAO> fieldExtractor =
                new BeanWrapperFieldExtractor<>();
        fieldExtractor.setNames(new String[] {"firstname", "dob"});
        fieldExtractor.afterPropertiesSet();

        // Questo oggetto ( che implementa ExtractorLineAggregator ) permette di convertire un oggetto in un elenco deliminato di stringhe. Il delimitatore
        // predefinito è una virgola

        DelimitedLineAggregator<UserDAO> lineAggregator =
                new DelimitedLineAggregator<>();
        lineAggregator.setDelimiter(",");
        // metodo derivato da ExtractorLineAggregator per l'estrazione di campo responsabile della suddivisione
        // di un oggetto di input in una matrice di oggetti
        lineAggregator.setFieldExtractor(fieldExtractor);

        // Questa classe è un writer di elementi che scrive i dati in un file o in un flusso.
        // Il writer fornisce anche il riavvio. La posizione del file di output è definita da - a Resource e deve rappresentare un file scrivibile.

        FlatFileItemWriter<UserDAO> flatFileItemWriter =
                new FlatFileItemWriter<>();
        flatFileItemWriter.setName("userItemWriter");
        flatFileItemWriter.setResource(
                new FileSystemResource("C:\\Users\\sdicostanzo\\Desktop\\myFile0.csv"));
        flatFileItemWriter.setLineAggregator(lineAggregator);

        return flatFileItemWriter;
    }

    @Bean
    public FlatFileItemReader<UserDAO> itemReader1() {

        // Impostiamo i field di destinazione derivandoli direttamente dalla classe UserDAO
        BeanWrapperFieldSetMapper<UserDAO> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(UserDAO.class);

        // Impostiamo il delimitatore come "," e andiamo ad impostare i nomi delle "colonne" del csv
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter(",");
        lineTokenizer.setNames(new String[] {"firstname", "dob"});

        // Andiamo ad impostare nel line mapper i valori dei field ( relativi alla classe) e alla tokenizzazione ( relativa alle righe del CSV )
        DefaultLineMapper<UserDAO> lineMapper = new DefaultLineMapper<>();
        lineMapper.setFieldSetMapper(fieldSetMapper);
        lineMapper.setLineTokenizer(lineTokenizer);

        // Impostiamo infine il file di riferimento dove andare ad effettuare la lettura impostando anche il line mapper
        FlatFileItemReader<UserDAO> flatFileItemReader = new FlatFileItemReader<>();
        flatFileItemReader.setName("userItemReader");
        flatFileItemReader.setResource(new PathResource("C:\\Users\\sdicostanzo\\Downloads\\myFile1.csv"));
        flatFileItemReader.setLineMapper(lineMapper);
        flatFileItemReader.setLinesToSkip(1);

        return flatFileItemReader;
    }

    @Bean
    public FlatFileItemWriter<UserDAO> itemWriter1() {

        // Questo oggetto è un estrattore di campo per un Java Beans. Dato un array di nomi di proprietà
        // chiamerà in modo riflessivo i getter sull'elemento e restituirà un array di tutti i valori
        BeanWrapperFieldExtractor<UserDAO> fieldExtractor =
                new BeanWrapperFieldExtractor<>();
        fieldExtractor.setNames(new String[] {"firstname", "dob"});
        fieldExtractor.afterPropertiesSet();

        // Questo oggetto ( che implementa ExtractorLineAggregator ) permette di convertire un oggetto in un elenco deliminato di stringhe. Il delimitatore
        // predefinito è una virgola

        DelimitedLineAggregator<UserDAO> lineAggregator =
                new DelimitedLineAggregator<>();
        lineAggregator.setDelimiter(",");
        // metodo derivato da ExtractorLineAggregator per l'estrazione di campo responsabile della suddivisione
        // di un oggetto di input in una matrice di oggetti
        lineAggregator.setFieldExtractor(fieldExtractor);

        // Questa classe è un writer di elementi che scrive i dati in un file o in un flusso.
        // Il writer fornisce anche il riavvio. La posizione del file di output è definita da - a Resource e deve rappresentare un file scrivibile.

        FlatFileItemWriter<UserDAO> flatFileItemWriter =
                new FlatFileItemWriter<>();
        flatFileItemWriter.setName("userItemWriter");
        flatFileItemWriter.setResource(
                new FileSystemResource("C:\\Users\\sdicostanzo\\Desktop\\myFile1.csv"));
        flatFileItemWriter.setLineAggregator(lineAggregator);

        return flatFileItemWriter;
    }




    @Bean
    public Flow splitFlow() {
        return new FlowBuilder<SimpleFlow>("splitFlow")
                .split(taskExecutor())
                .add(flow4(),flow5())
                .build();
    }



    @Bean
    public Flow flow1() {
        return new FlowBuilder<SimpleFlow>("flow1")
                .start(stepOne())
                .build();
    }

    @Bean
    public Flow flow2() {
        return new FlowBuilder<SimpleFlow>("flow2")
                .start(stepTwo())
                .build();
    }
    @Bean
    public Flow flow3() {
        return new FlowBuilder<SimpleFlow>("flow3")
                .start(stepThree())
                .build();
    }

    @Bean
    public Flow flow4() {
        return new FlowBuilder<SimpleFlow>("flow4")
                .start(stepFour())
                .build();
    }
    @Bean
    public Flow flow5() {
        return new FlowBuilder<SimpleFlow>("flow5")
                .start(stepFive())
                .build();
    }




    public Step stepOne() {
        return stepBuilderFactory
                .get("stepOne")
                .tasklet(
                        (StepContribution contribution, ChunkContext chunkContext) -> {
                            System.out.println("Il mio primo batch");
                            return RepeatStatus.FINISHED;
                        }
                )
                //.taskExecutor(taskExecutor())
                .build();
    }

    public Step stepTwo() {
        return stepBuilderFactory
                .get("stepTwo")
                .tasklet(
                        (StepContribution contribution, ChunkContext chunkContext) -> {
                            System.out.println("Sono il secondo Step");
                            final File folder = new File("C:\\Users\\sdicostanzo\\Desktop\\Prova_Batch_1");
                            listFiles(folder,"Thread 2");
                            System.out.println("Ho terminato il mio compito (2)");
                            return RepeatStatus.FINISHED;

                        }
                )
                //.taskExecutor(taskExecutor())
                .build();
    }

    public Step stepThree() {
        return stepBuilderFactory
                .get("stepThree")
                .tasklet(
                        (StepContribution contribution, ChunkContext chunk ) -> {
                            System.out.println("Sono il terzo Step");
                            final File folder = new File("C:\\Users\\sdicostanzo\\Desktop\\Prova_Batch_2");
                            listFiles(folder, "Thread 3");
                            System.out.println("Ho terminato il mio compito (3)");

                            return RepeatStatus.FINISHED;
                        }
                )
                //.taskExecutor(taskExecutor())
                .build();
    }

    public Step stepFour() {
        // E' stato impostato un buffer di lettura imposta chunck a 10 ( eseguira le operazioni 10 righe per volta )
        return stepBuilderFactory.get("Step Four")
                .<UserDAO,UserDAO>chunk(10)
                .reader(itemReader0())
                .writer(itemWriter0())
                .build();

    }

    public Step stepFive() {
        // E' stato impostato un buffer di lettura imposta chunck a 10 ( eseguira le operazioni 10 righe per volta )
        return stepBuilderFactory.get("Step Five")
                .<UserDAO,UserDAO>chunk(10)
                .reader(itemReader1())
                .writer(itemWriter1())
                .build();

    }



    public void listFiles(final File folder,String thread) {
        for (final File fileEntry : folder.listFiles()) {
            if (fileEntry.isDirectory()) {
                listFiles(fileEntry,thread);
            } else {
                System.out.println(fileEntry.getName() + " Thread: " + thread);
            }
        }
    }

}
