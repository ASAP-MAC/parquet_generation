library(dplyr)
library(ggplot2)
library(Polychrome)

# Concatenate input tables to single table with study_name included
concat_itabs <- function(fpaths) {
    itab_list <- lapply(fpaths, read.delim)
    names(itab_list) <- gsub(".tsv", "", fnames)
    
    itab_dat <- lapply(names(itab_list), function(x) {
        itab_list[[x]] %>%
            mutate(study_name = x) %>%
            select(study_name, sample_id)
    }) %>%
        bind_rows()
    
    return(itab_dat)
}

# Given the maximum number of samples that can fit into a single file, pack all
# provided samples into the smallest number of files while keeping samples from
# the same study together
pack_studies <- function(max_samples, sample_dat) {
    ## Get # of samples in each study
    study_counts <- count(sample_dat, study_name)
    
    ## Split studies larger than max_samples into multiple chunks
    study_chunks <- vector(mode = "list", length = nrow(study_counts)) |>
        setNames(study_counts$study_name)
    for (i in seq_len(nrow(study_counts))) {
        size <- study_counts$n[i]
        if (size > max_samples) {
            full_chunks <- size %/% max_samples
            remaining <- size %% max_samples
            if (remaining > 0) {
                study_chunks[[i]] <- c(rep(max_samples, full_chunks), remaining)
            } else {
                study_chunks[[i]] <- rep(max_samples, full_chunks)
            }
        } else {
            study_chunks[[i]] <- size
        }
    }
    
    ## Create dataframe for holding results
    results_df <- do.call(rbind, lapply(names(study_chunks), function(study) {
        data.frame(
            study = study,
            chunk_id = seq_along(study_chunks[[study]]),
            size = study_chunks[[study]],
            file_id = NA
        )
    }))
    rownames(results_df) <- NULL
    results_df <- arrange(results_df, desc(size))
    
    ## Assign chunks to files
    #files <- list()
    f_remaining <- c()
    
    for (i in seq_len(nrow(results_df))) {
        #study <- results_df$study[i]
        size <- results_df$size[i]
        #chunk_id <- results_df$chunk_id[i]
        
        assigned <- FALSE
        for (f in seq_along(f_remaining)) {
            if (f_remaining[f] >= size) {
                results_df$file_id[i] <- f
                f_remaining[f] <- f_remaining[f] - size
                assigned <- TRUE
                break
            }
        }
        if (!assigned) {
            results_df$file_id[i] <- length(f_remaining) + 1
            f_remaining <- c(f_remaining, max_samples - size)
        }
    }
    
    return(results_df)
}

plot_packing <- function(results_df) {
    cols_20 <- createPalette(20, c("#010101", "#ff0000"), range = c(50, 200))
    names(cols_20) <- unique(results_df$study)
    
    results_df <- results_df %>%
        mutate(file_id = factor(file_id, levels = sort(unique(file_id))))
    
    ggplot(results_df, aes(x = file_id, y = size, fill = study)) +
        geom_bar(stat = "identity", width = 0.8, color = "black", linewidth = 0.2) +
        geom_text(
            aes(label = size),
            position = position_stack(vjust = 0.5),
            size = 3,
            color = "black"
        ) +
        scale_y_continuous(expand = c(0, 0)) +
        scale_fill_manual(values = cols_20) +
        labs(
            x = "File ID",
            y = "Number of Samples",
            fill = "Study",
            title = "File Packing of Samples by Study"
        ) +
        theme_minimal(base_size = 12) +
        theme(
            panel.grid.major.x = element_blank(),
            panel.grid.minor = element_blank(),
            axis.text.x = element_text(angle = 45, hjust = 1),
            legend.position = "right",
            plot.title = element_text(face = "bold", size = 14, hjust = 0.5)
        )
}

## Get input table paths
itab_dir <- "/home/kaelyn/Desktop/Work/ASAP_MAC/pipeline/data/input_tables/kneaddata_v2"
# Exclude Payami_ studies
fnames <- list.files(itab_dir) %>%
    .[!grepl("Payami", .)]
fpaths <- file.path(itab_dir, fnames)

## Pack and visualize
all_tabs <- concat_itabs(fpaths)
packed_studies <- pack_studies(500, all_tabs)
plot_packing(packed_studies)

