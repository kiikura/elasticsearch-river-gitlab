package org.devtools.river;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;

public class GitlabClient {

	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub

		String token = "CPcomi7q3qyREs8wkpQz";
		URL url = new URL("http://demo.gitlab.com/api/v3/projects/2/issues/12/notes");
		URLConnection con = url.openConnection();
        con.addRequestProperty("User-Agent", "Gitlab River for Elasticsearch");
        con.addRequestProperty("PRIVATE-TOKEN", token);
        
        InputStream is = con.getInputStream();
        InputStreamReader reader = new InputStreamReader(is, "utf-8");
        BufferedReader r = new BufferedReader(reader);
        String line = null;
        while((line=r.readLine()) != null){
        	System.out.println(line);
        }
	}

}
