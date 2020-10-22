#! /bin/bash -l
#SBATCH --job-name=COVID__ivar
#SBATCH --output=logs/COVID__ivar_%A_%a.out
#SBATCH --time=168:00:00
#SBATCH --mem=48G
#SBATCH --cpus-per-task=2
conda activate py3
echo [`date`] Started job
samplesheet=/path/to/bams_sheet.txt
bamfile=$(cat $samplesheet  | sed -n "${SLURM_ARRAY_TASK_ID}p")
rsync -aL $bamfile $TMPDIR
sample=$(echo $(basename $bamfile) | sed -r "s/.bam//")
prefix=$sample
suffix=ivar
outdir=/athena/masonlab/scratch/projects/metagenomics/covid/analysis/ # fill in rest here...
echo [`date`] Sample is: $sample
cd $TMPDIR
if [ -f ${outdir}/${prefix}.ivar.fa ]; then
   return 1
fi
entries=$(samtools view ${prefix}.bam | wc -l)
if [[ $entries == 0 ]]; then
  return 1
fi
   
threads=2
min_quality=20
primer_bed_file=/home/cem2009/Projects/SARS-CoV-2/ivar/references/ARTIC-V1.bed
reference=/home/cem2009/Projects/SARS-CoV-2//reference//GCF_009858895.2_ASM985889v3_genomic_noPolyAtail.fna
if [ ! -f ${outdir}/${prefix}.${suffix}.bam ]; then
  echo "[1] Trimming the BAM"
  
  ivar \
    trim \
    -e \
    -q 0 \
    -i "${prefix}.bam" \
    -b "${primer_bed_file}" \
    -p "${prefix}.${suffix}"
fi
if [ ! -f ${outdir}/${prefix}.sorted.bam ]; then
  echo "[2] Sorting and indexing trimmed BAM"
  
  samtools \
    sort \
    -@ "${threads}" \
    "${prefix}.${suffix}.bam" \
    > "${prefix}.sorted.bam"
fi
if [ ! -f ${outdir}/${prefix}.sorted.bam.bai ]; then
  samtools \
    index \
    "${prefix}.sorted.bam"
  
fi
if [ ! -f ${outdir}/${prefix}.pileup ]; then
  echo "[3] Generating pileup"
  
  samtools \
    mpileup \
    --fasta-ref "${reference}" \
    --max-depth 250 \
    --count-orphans \
    --no-BAQ \
    --min-BQ 0 \
    -aa \
    "${prefix}.sorted.bam" \
    > "${prefix}.pileup"
fi
if [ ! -f ${outdir}/${prefix}.${suffix}.tsv ]; then
  echo "[4] Generating variants TSV"
  
  ivar \
    variants \
    -p "${prefix}.${suffix}" \
    -t 0.6 \
    -m 10 \
    < "${prefix}.pileup"
  
fi
if [ ! -f ${outdir}/${prefix}.${suffix}.fa ]; then
  # Generate consensus sequence with ivar
  echo "[5] Generating consensus sequence"
  
  ivar \
    consensus \
    -p "${prefix}.${suffix}" \
    -m 1 \
    -t 0.6 \
    -n N \
    < "${prefix}.pileup"
fi
rm *.bam*
rsync -avr * $outdir
echo [`date`] Finished job