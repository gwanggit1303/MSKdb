using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

[ApiController]
[Route("api/[controller]")]
    public class KdbController : ControllerBase
{
	private readonly ILogger<KdbController> _logger;
	private readonly KdbClient _kdbClient;

    public KdbController(ILogger<KdbController> logger, KdbClient kdbClient)
    {
        _logger = logger;
        _kdbClient = kdbClient;
    }
    
    
    [HttpGet("hello")]
    public IActionResult CallHelloWorldFunction()
    {
        _logger.LogInformation("Executing CallHelloWorldFunction method...");
        try
        {
            var result = _kdbClient.Execute("hello_world[]");
            _logger.LogInformation("Result from kdb+: {Result}", result);
            return Ok(result);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error calling hello_world function");
            return StatusCode(500, "Internal server error");
        }
    }
    
    [HttpGet("query")]
    public IActionResult ExecuteQuery([FromQuery] string q)
    {
        try
        {
            _logger.LogInformation(q);
            _logger.LogInformation("Executing ExecuteQuery method...");

            var result = _kdbClient.Execute(q);
            _logger.LogInformation("Result from kdb+: {Result}", result.ToString());
            
            return Ok(result);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error executing query");
            return StatusCode(500, "Internal server error");
        }
    }
    
    
   [HttpGet("parts")]
    public IActionResult GetParts()
    {
    
        try
        {
            _logger.LogInformation("Executing get all parts  method...");
	
            var parts = _kdbClient.GetParts();
            
	    if (parts.Count == 0)
		{
		    _logger.LogWarning("No parts retrieved from Kdb+");
		}
            
            return Ok(parts);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting all parts");
            return StatusCode(500, "Internal server error");
        }
    
    }    
    
     [HttpGet("statistics")]
        public IActionResult GetStatisticsForPartsQunantities()
        {
        
            try
            {
                _logger.LogInformation("Executing get statistics method...");
    	
                var partsQuant = _kdbClient.GetStatisticsForPartsQunantities();
                
    	    if (partsQuant.Count == 0)
    		{
    		    _logger.LogWarning("No parts quant result retrieved from Kdb+");
    		}
                
                return Ok(partsQuant);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error getting parts quant result ");
                return StatusCode(500, "Internal server error");
            }
        
    }    
    
        [HttpGet("suppliers")]
        public async Task<IActionResult> GetSupplierQuantities(string part)
        {
            if (string.IsNullOrEmpty(part))
            {
                return BadRequest("Part name is required");
            }

            try
            {
                _logger.LogInformation("Executing get part statistics method...");
    	
                var partsQuant = _kdbClient.GetStatisticsForPartsQunantity(part);
    	    if (partsQuant.Count == 0)
    		{
    		    _logger.LogWarning("No part quant result retrieved from Kdb+");
    		}
                
                return Ok(partsQuant);                
            }
            catch (Exception ex)
            {
                // Handle the exception (e.g., log it) and return an appropriate response
                return StatusCode(500, "Internal server error: " + ex.Message);
            }
        }    
    
}
