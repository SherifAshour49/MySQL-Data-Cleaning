--  Data Cleaning
-- 1.Remove duplicates
-- 2.Standardize the data
-- 3.Null values or blank values
-- 4.Remove any columns or rows


select * from layoffs_dataset;

create table layoffs_staging
like layoffs_dataset;

insert layoffs_staging
select * from layoffs_dataset;

select * from layoffs_staging;

with duplicates_CTE as (
select *, 
row_number() over ( partition by
 Company, 
 location, 
 industry, 
 total_laid_off, 
 percentage_laid_off, 
 `date`, 
 stage, 
 country, 
 funds_raised_millions) as row_num
from layoffs_staging)
 
 select * from duplicates_CTE where row_num >1
 ; 
 
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



select* from layoffs_staging2
;


insert into layoffs_staging2
select *, 
row_number() over ( partition by
 Company, 
 location, 
 industry, 
 total_laid_off, 
 percentage_laid_off, 
 `date`, 
 stage, 
 country, 
 funds_raised_millions) as row_num
from layoffs_staging
;


select industry
from layoffs_staging2
where industry like 'crypto%';

update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%';


select * from layoffs_staging2;


select distinct country from layoffs_staging2;


select * from layoffs_staging2
where country like 'united states_';

select distinct country, trim(TRAILING '.' FROM country)
from layoffs_staging2
order by 1;

update layoffs_staging2
set country = trim(TRAILING '.' FROM country)
where country like 'united states%';

select `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

alter table layoffs_staging2
modify column `date` date;

select * from layoffs_staging2;

select * from layoffs_staging2
where total_laid_off is null 
and percentage_laid_off is null;

select * from layoffs_staging2
where industry is null
or industry = '';

select * from layoffs_staging2
where company= 'Airbnb';

select t1.company, t1.industry, t2.company, t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
where t1.industry is null 
and t2.industry is not null;

update layoffs_staging2 
set industry = null 
where industry = '';

update layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null
;


select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;


alter table layoffs_staging2
drop column row_num;