const { Client, Query} = require('pg')
const creds = require('./creds.json')

const client = new Client({
    host: creds.host,
    user: creds.user,
    password: creds.password,
    port: creds.port,
    database: creds.database
  });

async function PK(){

        params=process.argv[2];
        if (process.argv.length==2) {
          console.log("Error: no parameters");
          process.exit();
        }
        else {
         l=params.split(";")
    
         param=[]
         for (i=0;i<l.length;i++)
           param[i]=l[i].split("=");
    
         for (i=0;i<param.length;i++)
           console.log(param[i]);
        }
    
        tname = param[0][1];
        console.log(tname);
        if(param[2][1]=='')
        {
                data=`${tname}: Invalid Input\n\n`
                await output(data)
                return
        }
        pk = [];
        for(i = 1; i < param[1].length; i++)
        pk = param[1][i];
        pk=pk.split(",")

    
        console.log(pk);
        if(pk.length>2)
        {
                data=`${tname}: Invalid Input\n\n`
                await output(data)
                return
        }

        columns = []; 
        for(i = 1; i < param[2].length; i++)
        columns = param[2][i];
        if(columns.length!=1)
                columns=columns.split(",")

        console.log(columns);

        var Query=`SELECT count(*) FROM ${tname}`
        sql(Query)
        q = await client.query(Query);
        if(q.rows[0].count==0)
        {
                data=`${tname}: Empty Table\n\n`
                await output(data)
                return    
        }

        Query=`SELECT count(*) FROM ${tname}`
        sql(Query)
        q = await client.query(Query);
        if(q.rows[0].count==1)
        {
                data=`\n${tname}: Single Row Table\n\n`
                await output(data)
                return    
        }
        
        Query=`SELECT count(distinct(${pk})) FROM ${tname}`
        sql(Query)
        q = await client.query(Query);
        test1=q.rows[0].count;
        Query=`SELECT count(*) FROM ${tname}`
        sql(Query)
        q = await client.query(Query);
        test2=q.rows[0].count;
        var ink;
        if(test1==test2){
        data=`${tname}\nPK\tY\n`
        ink=false;
        await FIRSTNF(ink)
        }
        else{
        data=`${tname}\nPK\tN\n`
        ink=true;
        await FIRSTNF(ink)
        }
}

async function FIRSTNF(ink){
        var Query=`SELECT ${pk}, ${columns},count(*) FROM ${tname} GROUP BY ${pk},${columns} HAVING COUNT(*)>1`
        sql(Query)
        try{
        FNF=await client.query(Query)
        }
        catch(error){
                data=`${tname}: Invalid Input\n\n`
                await output(data)
                return
        }
        if(FNF.rows.length>0){
        data=data+'1NF\tN\n2NF\tN\n3NF\tN\nBCNF\tN\n\n'
        await output(data)
        return
        }
        else{
        data=data+'1NF\tY\n'
        await SECONDNF(data,ink)
        return
        }
        
}

async function SECONDNF(data,ink){
        
        if(pk.length==1)
        {       data=data+'2NF\tY\n'
                await TNF(data)
                return
        }
        
        for(var i=0;i<pk.length;i++){
                for(var j=0;j<columns.length;j++){
                        var Query=`SELECT ${pk[i]},COUNT(DISTINCT ${columns[j]}) FROM ${tname} GROUP BY ${pk[i]} HAVING COUNT(DISTINCT ${columns[j]})>1;`
                        sql(Query)
                        q= await client.query(Query)
                        if(q.rows.length==0||ink){
                                data=data+'2NF\tN\n3NF\tN\nBCNF\tN\n\n'
                                await output(data)
                                return
                        }
                }
        }
        
        data=data+'2NF\tY\n'
        await TNF(data)
        return
}

async function TNF(data){
        var counts=0
        var ALLKEY
        
        for(var i=0;i<columns.length;i++){
                var Query=`SELECT COUNT(DISTINCT(${columns[i]})) FROM ${tname} HAVING COUNT(DISTINCT (${columns[i]}))>1;`
                sql(Query)
                q = await client.query(Query)
                if(q.rows[0]==undefined)
                        break;
                test1=q.rows[0].count
                Query= `SELECT count(*) FROM ${tname}`
                q = await client.query(Query)
                test2=q.rows[0].count
                if(test1==test2)
                        counts++             
        }
        for(var i=0;i<pk.length;i++){
                var Query=`SELECT COUNT(DISTINCT(${pk[i]})) FROM ${tname} HAVING COUNT(DISTINCT (${pk[i]}))>1;`
                sql(Query)
                q = await client.query(Query)
                if(q.rows[0]==undefined)
                        break;
                test1=q.rows[0].count
                Query= `SELECT count(*) FROM ${tname}`
                q = await client.query(Query)
                test2=q.rows[0].count
                if(test1==test2)
                        counts++     
        }
        if(counts==columns.length+pk.length)
                ALLKEY=true
        for(var i=0;i<columns.length;i++){
                for(var j=0;j<columns.length;j++){
                        if(i==j)
                                continue
                        var Query=`SELECT ${columns[i]},COUNT(DISTINCT (${columns[j]})) FROM ${tname} GROUP BY ${columns[i]} HAVING COUNT(DISTINCT (${columns[j]}))>1;`
                        sql(Query)
                        q= await client.query(Query)                  
                        if(q.rows.length==0&&!ALLKEY){
                                data=data+'3NF\tN\nBCNF\tN\n\n';
                                await output(data);
                                return;
                        }
                }
        }
        data=data+'3NF\tY\n'
        await BCNF(ALLKEY,data)
        return
}

async function BCNF(ALLKEY,data){


        for(var i=0;i<columns.length;i++){

                for(var j=0;j<pk.length;j++){
                        if(i==j)
                                continue
                        var Query=`SELECT ${columns[i]},COUNT(DISTINCT (${pk[j]})) FROM ${tname} GROUP BY ${columns[i]} HAVING COUNT(DISTINCT (${pk[j]}))>1;`
                        sql(Query)
                        q= await client.query(Query)
                        if(q.rows.length==0&&!ALLKEY){
                                data=data+'BCNF\tN\n\n'
                                await output(data)
                                return
                        }
                }
        }
        data=data+'BCNF\tY\n\n'
        await output(data)
        return

}



async function output(data){
                const fs = require('fs');
                let print = data;
                fs.appendFile('nf.txt', print, (err)=> {
                        if (err) throw err;
                })
        
}

async function sql(data){
                const fs = require('fs');
                let print = data+'\n';
                fs.appendFile('nf.sql', print, (err)=> {
                        if (err) throw err;
                })
        
}




async function main(){
        client.connect()
        await PK()
        

        client.end()
        process.exit
}


main()