SELECT location, date, total_cases, new_cases, total_deaths, population
FROM Netflix_covid_project..covid_deaths$
order by location, date;

--What perct. of people died from COVID?
SELECT location, date, total_cases, new_cases, total_deaths, population, round( ( (total_deaths/total_cases)*100), 3) as death_pct
from Netflix_covid_project..covid_deaths$
where location = 'India'
order by death_pct desc;

--Max death pct in India was 3.596% on 2020-04-12 
--between 2020-01-30 & 2022-02-12 (25 months)

select DATEDIFF(MONTH,min(date),max(date)) as time_bound_of_death_data from Netflix_covid_project..covid_deaths$;

--Affliction Rate of COVID in India between 2020-01-30 & 2022-02-12 (25 months)
select location, date, total_cases, new_cases, total_deaths, population, round((total_cases/population)*100, 3) as afflictionPct
from Netflix_covid_project..covid_deaths$
where location = 'India'
order by location, date;

select location, population, max(total_cases) as max_cases, date
from Netflix_covid_project..covid_deaths$
group by location, population
order by max_cases desc;

select cd.location, mc.max_cases, cd.date
from
(select location, max(total_cases) as max_cases 
from Netflix_covid_project..covid_deaths$
group by location) mc
inner join Netflix_covid_project..covid_deaths$ cd on cd.location = mc.location
group by cd.location,mc.max_cases, cd.date;

select mc.location as countries, max(mc.max_cases) as maxes, max(cd.date) as dates
from
(select location, max(total_cases) as max_cases 
from Netflix_covid_project..covid_deaths$
group by location) mc
inner join Netflix_covid_project..covid_deaths$ cd on cd.location = mc.location
group by mc.location
having max(cd.total_cases) = max(mc.max_cases);

select total_cases, location, date
from Netflix_covid_project..covid_deaths$
where location = 'United States'
order by total_cases desc

--Maximum Affliction Rates for all countries, ordered by Highest to Lowest
select location, max(date) as date, max(total_cases) as max_cases, max(population) as Population, 
	(max(total_cases)/max(population))*100 as afflictionPct
from Netflix_covid_project..covid_deaths$
group by location
order by afflictionPct desc;

--Latest total Death Count by Country 
select location, max(cast(total_deaths as int)) as latest_death_count
from Netflix_covid_project..covid_deaths$
where continent is not NULL --There are some values in Location which has continent names, and their corresponding Continent value is NULL
group  by location
having max(total_deaths/1) >= 0
order by latest_death_count desc;

-- Deaths per lakh people for all countries 
select location, round((max(cast(total_deaths as int))/max(population))*100000, 2) as Total_Deaths_per_lakh
from Netflix_covid_project..covid_deaths$
where continent is not NULL --There are some values in Location which has continent names, and their corresponding Continent value is NULL
group  by location, population
having max(total_deaths/1) >= 0
order by Total_Deaths_per_lakh desc;

--Latest total Death Count by Continent, ordered by highest to lowest death count
select location, population, max(cast(total_deaths as int)) as latest_death_count
from Netflix_covid_project..covid_deaths$
where continent is null and location not in ('World','Upper middle income','High income','Lower middle income','Low income')
group by location, population
order by latest_death_count desc;

--Latest total Death Count by Continent, per lakh people (last colm.)
select location, population, max(cast(total_deaths as int)) as latest_death_count, 
round((max(cast(total_deaths as int))/max(population))*100000, 2) as death_count_per_lakh
from Netflix_covid_project..covid_deaths$
where continent is null and location not in ('World','Upper middle income','High income','Lower middle income','Low income')
group by location, population
order by latest_death_count desc;

--Ranking countries in Asia by relative death count (desc.)
select location, max(cast(total_deaths as int)) as latest_death_count, 
round (max(cast(total_deaths as int))/max(population)*100000, 2) as total_deaths_per_lakh
from Netflix_covid_project..covid_deaths$
where continent = 'Asia'
group by location
order by total_deaths_per_lakh desc;

--total deaths in every continent
select continent, max(cast(total_deaths as int)) as latest_death_count
from Netflix_covid_project..covid_deaths$
where continent is not null
group by continent
order by latest_death_count desc;

--avg. life expectancy in every continent
select continent, round(avg(life_expectancy), 3) from Netflix_covid_project..covid_vacc$
where continent is not null
group by continent;

--Global datewise cases

--https://www.drugs.com/medical-answers/covid-19-symptoms-progress-death-3536264/'
--Day 18.5: The median time it takes from the first symptoms of COVID-19 to death is 18.5 days.

select date, sum(new_cases) as daily_case_count, round(sum(cast(new_deaths as int))/sum(new_cases)*1000, 0) as daily_death_to_cases_ratio
from Netflix_covid_project..covid_deaths$
group by date
having sum(new_cases) > 0
order by date;


--Running/Cumilitive total of vaccinations (for all countries, ordered by Continent, Country)

select cod.continent, cod.location,cod.date, cod.population, cov.new_vaccinations, 
sum(convert(bigint, cov.new_vaccinations)) over (partition by cod.location order by cod.location, cod.date) as cumilitiveNewVacc
from Netflix_covid_project..covid_deaths$ cod
inner join Netflix_covid_project..covid_vacc$ cov 
	on cod.location = cov.location 
	and cod.date = cov.date
where cod.continent is not null
order by cod.location, cod.date;


--Using a CTE(Common Table Expression)
With CumiltivePeopleVacc (continent, location, date, population, new_vaccinations, cumilitiveNewVacc)
as
(
select cod.continent, cod.location,cod.date, cod.population, cov.new_vaccinations, 
sum(convert(bigint, cov.new_vaccinations)) over (partition by cod.location order by cod.location, cod.date) as cumilitiveNewVacc
from Netflix_covid_project..covid_deaths$ cod
inner join Netflix_covid_project..covid_vacc$ cov 
	on cod.location = cov.location 
	and cod.date = cov.date
where cod.continent is not null
--order by cod.location, cod.date
)
select *, round((cumilitiveNewVacc/population)*100, 2) as PctPeopleVaccinated from CumiltivePeopleVacc
order by location,date;


--Using a Temp Table

--Rolling max of completely vaccinated people 
drop table if exists #CumilitivePeopleFullyVacc;
create table #CumilitivePeopleFullyVacc (
Continent varchar(255),
Location varchar(255),
Date datetime,
PeopleFullyVacc bigint,
CumilitivePeopleVacc bigint
);

insert into #CumilitivePeopleFullyVacc
select cod.continent, cod.location, cod.date, cov.people_fully_vaccinated,
sum(cast(cov.people_fully_vaccinated as bigint)) over(partition by cod.location order by cod.location, cod.date) as cumiltivePeopleVacc
from Netflix_covid_project..covid_deaths$ cod
	inner join Netflix_covid_project..covid_vacc$ cov on cod.location = cov.location 
	and cod.date = cov.date
where cod.continent is not null
order by cod.location, cod.date;

select * from #CumilitivePeopleFullyVacc
order by Location, Date;

--View for countries in Asia by relative death count
create view 
AsianCountriesByDeathToll as
select location, max(cast(total_deaths as int)) as latest_death_count, 
round (max(cast(total_deaths as int))/max(population)*100000, 2) as total_deaths_per_lakh
from Netflix_covid_project..covid_deaths$
where continent = 'Asia'
group by location;


