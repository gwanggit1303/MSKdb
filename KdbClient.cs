using Microsoft.Extensions.Logging;
using qSharp;
using System;
using System.Collections.Generic;

public class KdbClient
{
    private readonly QConnection _connection;
    private readonly ILogger<KdbClient> _logger;
   private readonly string _host;
    private readonly int _port;    

    public KdbClient(string host, int port, ILogger<KdbClient> logger)    {
        _logger = logger;
        _port = port;
        _logger = logger;        
        _connection = new QBasicConnection(host, port);
        _connection.Open();
    }



    public Object Execute(string query)
    {
        try
        {
  	_logger.LogInformation($"Conducting query: {query} on kdb");
        	
            var result = _connection.Sync(query);
            return result;
        }
        catch (Exception ex)
        {
            return $"Error: {ex.Message}";
        }
    }
    
  public List<Dictionary<string, object>> GetParts()
  {
	  var query = "select from p"; // Kdb+ query to select all from table p
	    var result = Execute(query);  
	  if (result is QKeyedTable keyedTable)
	    {
		var partsList = new List<Dictionary<string, object>>();
	    	 var keycolumns = keyedTable.Keys.Columns;
	    	  var valcolumns = keyedTable.Values.Columns;
				
              var rowsCount = keyedTable.Keys.RowsCount;
            		
            for (int idx = 0; idx < rowsCount; idx++)
            {
                var part = new Dictionary<string, object>();
                for (int j = 0; j < keycolumns.Length; j++)
                {
                    part[keycolumns[j]] = keyedTable.Keys[idx][j];
                }
                for (int j = 0; j < valcolumns.Length; j++)
                {
                    part[valcolumns[j]] = keyedTable.Values[idx][j];
                }
                
                partsList.Add(part);
            }            		
		
  	_logger.LogInformation($"Retrieved {partsList.Count} rows from table 'p'");
        return partsList;		
	    }
	    
	    
     _logger.LogWarning("No data retrieved or incorrect result type.");
    return new List<Dictionary<string, object>>();
  }


  public List<Dictionary<string, object>> GetStatisticsForPartsQunantities()
  {
	  var query = "select qty: sum qty, name: first name by p from sp lj `p xkey p"; // Kdb+ query to select quantities for each part in sp
	    var result = Execute(query);  
	  if (result is QKeyedTable keyedTable)
	    {
		var partQuanList = new List<Dictionary<string, object>>();
		
		partQuanList = convertQKeyedTableToListOfDic(keyedTable, partQuanList);
  	_logger.LogInformation($"Retrieved {partQuanList.Count} rows from table 'p'");
        return partQuanList;		
	    }
	    
	    
     _logger.LogWarning("No data retrieved or incorrect result type in method GetStatisticsForPartsQunantities .");
    return new List<Dictionary<string, object>>();
  }
  
  
  public List<Dictionary<string, object>> GetStatisticsForPartsQunantity(String part)
  {
	  //var query = "select sum qty by s.name from sp lj `s xkey sp.s where p=`{part}"; // Kdb+ query to select quantities for each part in sp
	  //var query = $"select sum qty by s.name from sp lj `s xkey sp.s where p=`{part}"; // Kdb+ query to select quantities for each part in sp
	  
	  var query = $"select qty: sum qty, name: first name by s from sp lj `s xkey s where p= `{part}";
	    var result = Execute(query);  
	  if (result is QKeyedTable keyedTable)
	    {
		var partQuanList = new List<Dictionary<string, object>>();
		
		partQuanList = convertQKeyedTableToListOfDic(keyedTable, partQuanList);
  	_logger.LogInformation($"Retrieved {partQuanList.Count} rows from table 'sp'");
        return partQuanList;		
	    }
	    
	    
     _logger.LogWarning("No data retrieved or incorrect result type in method GetStatisticsForPartsQunantity .");
    return new List<Dictionary<string, object>>();
  }
  
  
  private List<Dictionary<string, object>> convertQKeyedTableToListOfDic(QKeyedTable keyedTable, List<Dictionary<string, object>> listOfDic)
  {
  
  	    	 var keycolumns = keyedTable.Keys.Columns;
  	    	  var valcolumns = keyedTable.Values.Columns;
  				
                var rowsCount = keyedTable.Keys.RowsCount;
              		
              for (int idx = 0; idx < rowsCount; idx++)
              {
                  var dic = new Dictionary<string, object>();
                  for (int j = 0; j < keycolumns.Length; j++)
                  {
                      dic[keycolumns[j]] = keyedTable.Keys[idx][j];
                  }
                  for (int j = 0; j < valcolumns.Length; j++)
                  {
                      dic[valcolumns[j]] = keyedTable.Values[idx][j];
                  }
                  
                  listOfDic.Add(dic);
              }            		
		return listOfDic;
  
  
  }
  
  
  

 public void Close()
    {
        _connection.Close();
    }




}
