import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.Scanner;

public class DataClient extends Thread{
	static int processid = 0;
	static int portno = 0;

	public static int noofservers = 0;
	public static int noofclients = 0;
	public static int noofmessages = 0;
	public static int Time_Unit = 0;
	public static int[] failingnodes;
	
	public static int noofdataobj= 4;
	public static int valuetoappend= 1;
	
	public static int unsuccessfulAccess= 0;
	public static int successfulAccess= 0;
	public static int sentmsgs= 0;
	public static int withdrawmsgs= 0;
	public static int commitmsgs= 0;
	public static int a = 0; //root node
	public static long sum = 0; 
	public static long avg = 0; 
	
	public static Object lock = new Object();
	
	static boolean[] grantArray = new boolean[7];
	public static ArrayList<Long> stats = new ArrayList<Long>();

	public static HashMap<String, PrintWriter> socketMap = new HashMap<String, PrintWriter>();
	//public static ArrayList<String> messageQueue = new ArrayList<String>();
	public static Socket processSocket;

	public static void main(String[] args) throws IOException, InterruptedException {
		argParsing(args);//readinputfile and argparsing
		parseInput();
		
		System.out.println(noofservers);
		System.out.println(noofclients );
		System.out.println(noofmessages);
		System.out.println(Time_Unit );
		for(int i=0;i<failingnodes.length;i++){
			System.out.println(failingnodes[i]);
		}
		
		establishClientConnections();
		startAlgo();
		System.out.println("-----------************-------------");
	}

	//for parsing the arguments
	public static void argParsing(String[] args) throws IOException {
		String firstArg  = args[0];
		String secondArg = args[1];
		processid =Integer.parseInt(firstArg);
		portno = Integer.parseInt(secondArg);
		System.out.println("Process ID of this system is : "+processid);
	}

	public static void startAlgo() throws InterruptedException {
		System.out.println("Starting algo");
		
		for(int i=0;i<noofmessages;i++){
			Random rand = new Random();
			int dataObjRequested = rand.nextInt(noofdataobj);//randomly select the data object 
			
			if(processid == 7){
				if(i == noofmessages/5){
					sendDeactivate();
				}
				if(i == 2*(noofmessages/5)){
					sendReactivate();
					Thread.sleep(60000);
				}
			}
			else{
				if(i == 2*(noofmessages/5)){
					Thread.sleep(60000);
				}
			}
						
			sendRequest(dataObjRequested);
			System.out.println("waiting for awaiting grant time");
			//Thread.sleep(20*Time_Unit);
			long startTimer = System.currentTimeMillis();
			while( (System.currentTimeMillis() <startTimer+(20*Time_Unit)) && !checkQuorum(a)){
				Thread.sleep(50);
			}
			
			if(checkQuorum(a)){
				long timeTakenforReplies = System.currentTimeMillis() - startTimer ;
				stats.add(timeTakenforReplies);
				System.out.println("inside if checkquorum");
				System.out.println("waiting for hold time");
				Thread.sleep(Time_Unit);
				sendCommit(dataObjRequested);
			}
			else{
				System.out.println("inside if not checkquorum");
				sendWithdraw(dataObjRequested);
			}
		}
		System.out.println("no of unsuccessful messages: "+unsuccessfulAccess);
		System.out.println("no of successful messages: "+successfulAccess);
		System.out.println("no of sent messages: "+sentmsgs);
		System.out.println("no of committed messages: "+commitmsgs);
		System.out.println("no of withdraw messages: "+withdrawmsgs);
		calcstats();
		System.out.println("Algo finished");
		
	}

	public static void calcstats() {
		for(int i=0;i<stats.size();i++){
			sum = sum + stats.get(i);
		}
		long avg = sum/commitmsgs;
		System.out.println("the average of successful accesses: "+ avg);
	}

	public static void sendDeactivate() {
		System.out.println("entering sendDeactivate");
		for(int i=0;i<failingnodes.length;i++){
			String sendMesg = "DEACTIVATE"+" "+processid+" "+0+" "+valuetoappend;
			PrintWriter newPW = socketMap.get(Integer.toString(failingnodes[i]));
			newPW.println(sendMesg);
		}
		System.out.println("exiting sendDeactivate");
	}

	public static void sendReactivate() {
		System.out.println("entering sendReactivate");
		for(int i=0;i<failingnodes.length;i++){
			String sendMesg = "REACTIVATE"+" "+processid+" "+0+" "+valuetoappend;
			PrintWriter newPW = socketMap.get(Integer.toString(failingnodes[i]));
			newPW.println(sendMesg);
		}
		System.out.println("exiting sendReactivate");
	}

	public static void sendWithdraw(int dataObjRequested) {
		System.out.println("entering send withdraw");
		for(int i=0;i<noofservers;i++){
			String sendMesg = "WITHDRAW"+" "+processid+" "+dataObjRequested+" "+valuetoappend;
			PrintWriter newPW = socketMap.get(Integer.toString(i));
			newPW.println(sendMesg);
			withdrawmsgs++;
		}
		unsuccessfulAccess++;
		System.out.println("exiting send withdraw");
	}

	public static void sendCommit(int dataObjRequested) {
		System.out.println("entering send commit");
		for(int i=0;i<noofservers;i++){
			String sendMesg = "COMMIT"+" "+processid+" "+dataObjRequested+" "+valuetoappend;
			PrintWriter newPW = socketMap.get(Integer.toString(i));
			newPW.println(sendMesg);
			commitmsgs++;
		}
		successfulAccess++;
		System.out.println("exiting send commit");
	}

	public static void sendRequest(int dataObjRequested) {
		System.out.println("entering send request");
		synchronized (lock) {
			System.out.println("before sending request intialize grantArray to false");
			for(int i=0;i<grantArray.length;i++){
				grantArray[i]=false;
			}
		}
		for(int i=0;i<noofservers;i++){
			String sendMesg = "REQUEST"+" "+processid+" "+dataObjRequested+" "+valuetoappend;
			//System.out.println(socketMap.size());
			PrintWriter newPW = socketMap.get(Integer.toString(i));
			//System.out.println(newPW);
			newPW.println(sendMesg);
			sentmsgs++;
		}
		System.out.println("exiting send request");
	}

	private static boolean checkQuorum(int a){
		if(a >= ((noofservers-1)/2))
			return grantArray[a];
		if(( grantArray[a] && checkQuorum((2*a)+1) ) || ( grantArray[a] && checkQuorum((2*a)+2) ) || ( checkQuorum((2*a)+1) && checkQuorum((2*a)+2) ) )
			return true;
		else
			return false;		
	}

	public static void establishClientConnections() throws IOException{
		System.out.println("inside establish connections");
		for(int i=0;i<noofservers;i++){
			String smachineName = readConfig(Integer.toString(i));
			InetAddress processIPAddr = InetAddress.getByName(smachineName);  // Fetching the IP address from the hostname
			//System.out.println(processIPAddr);
			processSocket = new Socket(processIPAddr, portno);
			PrintWriter pwProcess = new PrintWriter(processSocket.getOutputStream(), true);
			System.out.println("connection to "+i+" is created");
			pwProcess.println("PING" + " " + processid);
			
			ReceiveThread connection = new ReceiveThread(processSocket, i);
			Thread t = new Thread(connection);
			t.start();
			socketMap.put(Integer.toString(i), pwProcess);
		}
		System.out.println("Done with Connections .. Can start algorithm now");
	}

	public static String readConfig(String dest) throws FileNotFoundException {
		File configfile = new File("config.txt");
		Scanner configip = new Scanner(configfile);
		String machineName="";
		while(configip.hasNext()) {
			String nxtLine = configip.nextLine();
			String[] tokens = nxtLine.split(" ");
			if(tokens[0].equals(dest)){
				machineName = tokens[1];
			}
		}
		configip.close();
		return machineName;
	}
	
	public static void parseInput() throws FileNotFoundException {
		File configfile = new File("input.txt");
		Scanner reader = new Scanner(configfile);
		while(reader.hasNextLine()){
			String line = reader.nextLine();
			if(!line.trim().startsWith("#")){
				if(line.startsWith("NS=")){
					noofservers = Integer.parseInt(line.substring(line.indexOf('=')+1).trim());
				}else if(line.startsWith("NC=")){
					noofclients = Integer.parseInt(line.substring(line.indexOf('=')+1).trim());
				}else if(line.startsWith("M=")){
					noofmessages = Integer.parseInt(line.substring(line.indexOf('=')+1).trim());
				}else if(line.startsWith("TIME_UNIT=")){
					Time_Unit = Integer.parseInt(line.substring(line.indexOf('=')+1).trim());
				}else if(line.startsWith("FAILINGNODES:")){
					line = reader.nextLine();

					String [] failNodes = line.split(" ");
					failingnodes = new int[failNodes.length];
					for(int i=0;i<failNodes.length;i++)
						failingnodes[i]= Integer.parseInt(failNodes[i]);
				}
			}
		}
		reader.close();
	}
}

//====================================================================================================================

class ReceiveThread implements Runnable{
	static Socket socket;
	static String msgToServer=null;
	static int replyFrom = 0;

	public ReceiveThread(Socket s, int i) {
		System.out.println("Starting Receiver Thread for process: "+i);
		this.socket=s;
		this.replyFrom = i;
		System.out.println("after constructor");
	}

	@Override
	public void run() {
		try {
			//System.out.println("inside run");
			BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			//System.out.println("after buffer reader");
			while(true)
			{
				//System.out.println("inside while");
				try
				{
					msgToServer = in.readLine();
					if(null != msgToServer){
						//System.out.println("-------------------"+msgToServer+"--------------------");
						String[] split = msgToServer.split(" ");
						if(split[0].equals("GRANT")){
							System.out.println(msgToServer);
							synchronized (DataClient.lock) {
								//DataClient.messageQueue.add(msgToServer);
								DataClient.grantArray[Integer.parseInt(split[1])] = true;
							}
						}
						else{
							System.out.println(msgToServer);
						}
					}
				}catch (IOException e) {
					e.printStackTrace();
				}
			}
		}catch (IOException e) {
			e.printStackTrace();
		}
	}
}