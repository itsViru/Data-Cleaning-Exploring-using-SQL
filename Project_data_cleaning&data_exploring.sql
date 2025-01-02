
-- 1. Remove Duplicates
-- 2. Standardize the data
-- 3. Null Values or blank values
-- 4. Remove any coulumns 

-- 1. Remove Duplicates

select*
from layoffs;

create table layoffs_staging
like layoffs;

insert layoffs_staging
select*
from layoffs;

select*
from layoffs_staging;


with duplicate_cte as
(
select*,
row_number() over(
partition by company, industry, total_laid_off, percentage_laid_off, `date`, country, funds_raised_millions) as row_num
from layoffs_staging
)
select*
from duplicate_cte
where row_num > 1;


SHOW CREATE TABLE layoffs_staging;





CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

 


select*
from layoffs_staging2;


insert into layoffs_staging2
select*,
row_number() over(
partition by company, industry, total_laid_off, percentage_laid_off, `date`, country, funds_raised_millions) as row_num
from layoffs_staging;

SET SQL_SAFE_UPDATES = 0;
delete
from layoffs_staging2
where row_num>1;

select*
from layoffs_staging2;

select*
from layoffs_staging2
where row_num>1;

-- 2. Standardize the data
Select company, Trim(company)
from layoffs_staging2;

update layoffs_staging2
set  company = trim(company);

select distinct industry
from layoffs_staging2
order by 1 ;

select *
from layoffs_staging2
where industry like 'Crypto%';

update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

select distinct country, trim(trailing '.' from country)
from layoffs_staging2
order by 1;

update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'United States%';

select `date`
from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

alter table layoffs_staging2
modify column  `date` date;

select *
from layoffs_staging2;


-- 3. Null Values or blank values

select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

update layoffs_staging2
set industry = null
where industry = '';

select *
from layoffs_staging2
where industry is null
or industry = '';

select *
from layoffs_staging2
where company like 'Airbnb';

select t1.industry, t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

update layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
set t1.industry = t2.industry
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

select *
from layoffs_staging2
where percentage_laid_off = 1;
 

select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

delete
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

alter table layoffs_staging2
drop column row_num;


select *
from layoffs_staging2;




SHOW CREATE TABLE layoffs_staging2;



ALTER TABLE layoffs_staging2 ADD COLUMN pk_id INT AUTO_INCREMENT PRIMARY KEY;

ALTER TABLE layoffs_staging2
ADD COLUMN row_num2 INT NULL;

WITH cte AS (
    SELECT
      pk_id,
      ROW_NUMBER() OVER (
         PARTITION BY company, location, industry, total_laid_off, 
                        percentage_laid_off, `date`, stage, country, funds_raised_millions
         ORDER BY pk_id
      ) AS rn
    FROM layoffs_staging2
)
UPDATE layoffs_staging2 AS tgt
JOIN cte
  ON tgt.pk_id = cte.pk_id
SET tgt.row_num2 = cte.rn;

DELETE
FROM layoffs_staging2
WHERE row_num2 > 1;

select *
from layoffs_staging2;

select *
FROM layoffs_staging2
WHERE row_num2 > 1;

DELETE
FROM layoffs_staging2
WHERE row_num2 > 1;


ALTER TABLE layoffs_staging2
DROP COLUMN row_num2;


ALTER TABLE layoffs_staging2
DROP COLUMN pk_id;

-- Exploratory Data Analysis

select max(total_laid_off), max(percentage_laid_off)
from layoffs_staging2;

select *
from layoffs_staging2
where percentage_laid_off = 1
order by funds_raised_millions desc;

select company, sum(total_laid_off)
from layoffs_staging2
group by company
order by 2 desc;

select min(`date`), max(`date`)
from layoffs_staging2;


select country, sum(total_laid_off)
from layoffs_staging2
group by country
order by 2 desc;

select year(`date`), sum(total_laid_off)
from layoffs_staging2
group by year(`date`)
order by 1 desc;

select stage, sum(total_laid_off)
from layoffs_staging2
group by stage
order by 2 desc;


select company, avg(percentage_laid_off)
from layoffs_staging2
group by company
order by 2 desc;


select substring(`date`,1,7) as `month`, sum(total_laid_off)
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `month`
order by 2 asc;

with Rolling_total as
(
select substring(`date`,1,7) as `month`, sum(total_laid_off) as total_off
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `month`
order by 1 asc
)
select `month`, total_off,
sum(total_off) over(order by `month`) as rolling_total
from Rolling_total;

select company, year(`date`), sum(total_laid_off)
from layoffs_staging2
group by company,  year(`date`)
order by 3 desc;

with company_year (company, years, total_laid_off) as
(
select company, year(`date`), sum(total_laid_off)
from layoffs_staging2
group by company, year(`date`)
), company_year_rank as
(select*,
dense_rank() over(partition by years order by total_laid_off desc) as Ranking
from company_year
where years is not null
)
select *
from company_year_rank
where ranking <= 5;

