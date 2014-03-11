import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Scanner;

public class DataServer extends Thread{
	static int processid = 0;
	static int portno = 0;

	public static int noofservers = 0;
	public static int noofclients = 0;
	public static int noofmessages = 0;
	public static int Time_Unit = 0;
	public static int[] failingnodes;
	
	public int noofdataobj= 4;
	public static Socket processSocket;
	public static boolean isActive = true; 

	public static int grantmsgs = 0;
	public static int ackmsgs = 0;
	//create queues for each data object
	public static Queue<String> q0 = new LinkedList<String>();//queue for d0
	public static Queue<String> q1 = new LinkedList<String>();//queue for d1
	public static Queue<String> q2 = new LinkedList<String>();//queue for d0
	public static Queue<String> q3 = new LinkedList<String>();//queue for d0
	//private Lock lock =  new ReentrantLock();//reentrant lock
	public static DataObject d0 = null;
	public static DataObject d1 = null;
	public static DataObject d2 = null;
	public static DataObject d3 = null;
	
	public static HashMap<String, PrintWriter> socketMap = new HashMap<String, PrintWriter>();

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
		
		//startServerThread();//start
		
		Thread serverThread = new Thread(){
			public void run(){
				try {
					ServerSocket serverSocket = new ServerSocket(portno);
					System.out.println("Starting Server Thread");
					Socket socket= null;
					while(true){
						socket = serverSocket.accept();
						//System.out.println("Creating receiver thread from server socket thread");
						BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
						String msgToServer = in.readLine();
						String msg[] = msgToServer.split(" ");
						System.out.println(msgToServer);
						if(msg[0].equals("DATA")){
							System.out.println("-------------------"+msgToServer+"--------------------");
							startAlgo(msgToServer);
						}
						System.out.println("Putting socket of "+ msg[1] +" in the socketmap");
						PrintWriter pwProcess = new PrintWriter(socket.getOutputStream(), true);
						//updateSocketMap(msg[2], pwProcess);
						socketMap.put(msg[1], pwProcess);

						ReceiveThread1 connection = new ReceiveThread1(socket, Integer.parseInt(msg[1]));
						Thread connThread = new Thread(connection);
						connThread.start();
						//pingCounter++;
					}
				}
				catch (IOException e) {
					e.printStackTrace();
				}
			}
		};
		serverThread.start();
		//Thread.sleep(20000);
		
		//establishClientConnections();//establish connections with the other servers
		
		d0 = new DataObject(3, 1);
		d1 = new DataObject(2, 1);
		d2 = new DataObject(1, 1);
		d3 = new DataObject(0, 1);
		
		System.out.println("waiting for the incoming messages");
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

	//for parsing the arguments
	public static void argParsing(String[] args) throws IOException {
		String firstArg  = args[0];
		String secondArg = args[1];
		processid =Integer.parseInt(firstArg);
		portno = Integer.parseInt(secondArg);
		System.out.println("Process ID of this system is : "+processid);
	}

	public static void startAlgo(String messageToServer) throws IOException {
		System.out.println("starting algo");
		String[] servMesg = messageToServer.split(" ");
		if(servMesg[0].equals("REQUEST") && isActive){
			System.out.println("inside if request");
			if(servMesg[2].equals("0")){
				System.out.println("inside if D0");
				if(q0.size() == 0){//if the queue0 for data object is empty
					System.out.println("inside if q0 size is 0");
					sendGrant(servMesg[1], servMesg[2], servMesg[3]);//send grant to client
					q0.add(messageToServer);//add the message to the queue0
					//enters blocked state on d0
				}
				else{//if the queue is not empty then put it in a queue0
					System.out.println("inside if q0 size is not 0");
					q0.add(messageToServer);
				}
			}
			else if(servMesg[2].equals("1")){
				System.out.println("inside if D1");
				if(q1.size() == 0){//if the queue1 for data object is empty
					System.out.println("inside if q1 size is 0");
					sendGrant(servMesg[1], servMesg[2], servMesg[3]);//send grant to client
					q1.add(messageToServer);//add the message to the queue1
					//enters blocked state on d1
				}
				else{//if the queue is not empty then put it in a queue1
					System.out.println("inside if q1 size is not 0");
					q1.add(messageToServer);
				}
			}
			else if(servMesg[2].equals("2")){
				System.out.println("inside if D2");
				if(q2.size() == 0){//if the queue1 for data object is empty
					System.out.println("inside if q2 size is 0");
					sendGrant(servMesg[1], servMesg[2], servMesg[3]);//send grant to client
					q2.add(messageToServer);//add the message to the queue1
					//enters blocked state on d1
				}
				else{//if the queue is not empty then put it in a queue1
					System.out.println("inside if q2 size is not 0");
					q2.add(messageToServer);
				}
			}
			else if(servMesg[2].equals("3")){
				System.out.println("inside if D3");
				if(q3.size() == 0){//if the queue1 for data object is empty
					System.out.println("inside if q3 size is 0");
					sendGrant(servMesg[1], servMesg[2], servMesg[3]);//send grant to client
					q3.add(messageToServer);//add the message to the queue1
					//enters blocked state on d1
				}
				else{//if the queue is not empty then put it in a queue1
					System.out.println("inside if q3 size is not 0");
					q3.add(messageToServer);
				}
			}
		}
		else if(servMesg[0].equals("COMMIT") && isActive){
			System.out.println("inside if commit");
			if(servMesg[2].equals("0")){
				System.out.println("inside if D0");
				d0.setValue(Integer.parseInt(servMesg[3]));//change the value and version
				d0.setVersion();
				System.out.println("The Value and Version of do are: "+ d0.getValue() + "--" + d0.getVersion());
				sendAck(servMesg[1], servMesg[2], servMesg[3]);//send ack to client
				System.out.println("Removing the committed req from q0: "+ q0.poll());//remove the client req
				if(q0.peek() != null){
					System.out.println("inside if q0.peek not null");
					String reqInQueue = q0.peek();
					String[] queueReq = reqInQueue.split(" ");
					sendGrant(queueReq[1], queueReq[2], queueReq[3]);
				}
			}
			else if(servMesg[2].equals("1")){
				System.out.println("inside if D1");
				d1.setValue(Integer.parseInt(servMesg[3]));//change the value and version
				d1.setVersion();
				System.out.println("The Value and Version of d1 are: "+ d1.getValue() + "--" + d1.getVersion());
				sendAck(servMesg[1], servMesg[2], servMesg[3]);//send ack to client
				System.out.println("Removing the committed req from q1: "+ q1.poll());//remove the client req
				if(q1.peek() != null){
					System.out.println("inside if q1.peek is not null");
					String reqInQueue = q1.peek();
					String[] queueReq = reqInQueue.split(" ");
					sendGrant(queueReq[1], queueReq[2], queueReq[3]);
				}
			}
			else if(servMesg[2].equals("2")){
				System.out.println("inside if D2");
				d2.setValue(Integer.parseInt(servMesg[3]));//change the value and version
				d2.setVersion();
				System.out.println("The Value and Version of d2 are: "+ d2.getValue() + "--" + d2.getVersion());
				sendAck(servMesg[1], servMesg[2], servMesg[3]);//send ack to client
				System.out.println("Removing the committed req from q2: "+ q2.poll());//remove the client req
				if(q2.peek() != null){
					System.out.println("inside if q2.peek is not null");
					String reqInQueue = q2.peek();
					String[] queueReq = reqInQueue.split(" ");
					sendGrant(queueReq[1], queueReq[2], queueReq[3]);
				}
			}
			else if(servMesg[2].equals("3")){
				System.out.println("inside if D3");
				d3.setValue(Integer.parseInt(servMesg[3]));//change the value and version
				d3.setVersion();
				System.out.println("The Value and Version of d3 are: "+ d3.getValue() + "--" + d3.getVersion());
				sendAck(servMesg[1], servMesg[2], servMesg[3]);//send ack to client
				System.out.println("Removing the committed req from q3: "+ q3.poll());//remove the client req
				if(q3.peek() != null){
					System.out.println("inside if q3.peek is not null");
					String reqInQueue = q3.peek();
					String[] queueReq = reqInQueue.split(" ");
					sendGrant(queueReq[1], queueReq[2], queueReq[3]);
				}
			}
		}
		else if(servMesg[0].equals("WITHDRAW") && isActive){
			System.out.println("inside if withdraw");
			if(servMesg[2].equals("0")){
				System.out.println("inside if D0");
				System.out.println("queue size before removing: "+q0.size());
				q0.remove("REQUEST"+" "+servMesg[1]+" "+servMesg[2]+" "+servMesg[3]);
				System.out.println("queue size after removing: "+q0.size());
			}
			else if(servMesg[2].equals("1")){
				System.out.println("inside if D1");
				System.out.println("queue size before removing: "+q1.size());
				q1.remove("REQUEST"+" "+servMesg[1]+" "+servMesg[2]+" "+servMesg[3]);
				System.out.println("queue size after removing: "+q1.size());
			}
			else if(servMesg[2].equals("2")){
				System.out.println("inside if D2");
				System.out.println("queue size before removing: "+q2.size());
				q2.remove("REQUEST"+" "+servMesg[1]+" "+servMesg[2]+" "+servMesg[3]);
				System.out.println("queue size after removing: "+q2.size());
			}
			else if(servMesg[2].equals("3")){
				System.out.println("inside if D3");
				System.out.println("queue size before removing: "+q3.size());
				q3.remove("REQUEST"+" "+servMesg[1]+" "+servMesg[2]+" "+servMesg[3]);
				System.out.println("queue size after removing: "+q3.size());
			}
		}
		else if(servMesg[0].equals("DEACTIVATE")){
			System.out.println("inside if deactivate");
			isActive = false;
			q0.clear();
			q1.clear();
			q2.clear();
			q3.clear();
		}
		else if(servMesg[0].equals("REACTIVATE")){
			System.out.println("inside if reactivate");
			isActive = true;
			updatingMyself();
		}
		else if(servMesg[0].equals("POLL")){
			System.out.println("inside if poll");
			sendDataObject(servMesg);
		}
		else if(servMesg[0].equals("DATA")){
			System.out.println("inside if data");
			update(servMesg);
		}
	}

	public static void update(String[] servMesg) {
		System.out.println("entering update");
		d0.updateValue(Integer.parseInt(servMesg[2]));
		d0.updateVersion(Integer.parseInt(servMesg[3]));
		d1.updateValue(Integer.parseInt(servMesg[4]));
		d1.updateVersion(Integer.parseInt(servMesg[5]));
		d2.updateValue(Integer.parseInt(servMesg[4]));
		d2.updateVersion(Integer.parseInt(servMesg[5]));
		d3.updateValue(Integer.parseInt(servMesg[4]));
		d3.updateVersion(Integer.parseInt(servMesg[5]));
		System.out.println("updated values and versions of d0, d1, d2 and d3 are: "+d0.getValue()+":"+d0.getVersion()+"##"+d1.getValue()+":"+d1.getVersion()+"##"+d2.getValue()+":"+d2.getVersion()+"##"+d3.getValue()+":"+d3.getVersion());
		System.out.println("exiting update");
	}

	public static void sendDataObject(String[] servMesg) throws IOException {
		System.out.println("entering sendDataObject");
		String sendMesg = "DATA"+" "+processid+" "+d0.getValue()+" "+d0.getVersion()+" "+d1.getValue()+" "+d1.getVersion()+" "+d2.getValue()+" "+d2.getVersion()+" "+d3.getValue()+" "+d3.getVersion();
		int i = Integer.parseInt(servMesg[1]);
		String smachineName = readConfig(Integer.toString(i));
		InetAddress processIPAddr = InetAddress.getByName(smachineName);  // Fetching the IP address from the hostname
		processSocket = new Socket(processIPAddr, portno);
		PrintWriter pwProcess = new PrintWriter(processSocket.getOutputStream(), true);
		System.out.println("connection to server " +i+ " is created");
		pwProcess.println(sendMesg);
		pwProcess.println(sendMesg);
		
		System.out.println("exiting sendDataObject");
	}

	public static void updatingMyself() throws IOException {
		System.out.println("entering updatingMyself");
		String sendMesg = "POLL"+" "+processid+" "+0+" "+0;
		int i = 0;
		String smachineName = readConfig(Integer.toString(i));
		InetAddress processIPAddr = InetAddress.getByName(smachineName);  // Fetching the IP address from the hostname
		processSocket = new Socket(processIPAddr, portno);
		PrintWriter pwProcess = new PrintWriter(processSocket.getOutputStream(), true);
		System.out.println("connection to server " +i+ " is created");
		pwProcess.println(sendMesg);
		pwProcess.println(sendMesg);
		
		ReceiveThread connection = new ReceiveThread(processSocket, i);
		Thread t = new Thread(connection);
		t.start();
		System.out.println("exiting updatingMyself");
	}

	public static void sendAck(String clientid, String dataObjRequested, String valuetoappend) {
		System.out.println("entering sendAck");
		String sendMesg = "ACK"+" "+processid+" "+dataObjRequested+" "+valuetoappend;
		PrintWriter newPW = socketMap.get(clientid);
		newPW.println(sendMesg);
		ackmsgs++;
		System.out.println("no of ack messages till now: "+ackmsgs);
		System.out.println("exiting sendAck");
	}

	public static void sendGrant(String clientid, String dataObjRequested, String  valuetoappend) {
		System.out.println("entering sendGrant");
		String sendMesg = "GRANT"+" "+processid+" "+dataObjRequested+" "+valuetoappend;
		PrintWriter newPW = socketMap.get(clientid);
		newPW.println(sendMesg);
		grantmsgs++;
		System.out.println("no of grant messages till now: "+grantmsgs);
		System.out.println("exiting sendGrant");
	}
}
//============================================================================================
class DataObject{
	private int value;
	private int version;

	public DataObject(int val, int ver){
		//System.out.println("entering DataObject");
		value = val;
		version = ver;
		//System.out.println("exiting DataObject");
	}

	public int getValue(){
		return value;
	}
	public int getVersion(){
		return version;
	}
	public void setValue(int val){
		value = value + val;
	}
	public void setVersion(){
		version++;
	}
	public void updateValue(int val){
		value = val;
	}
	public void updateVersion(int ver){
		version = ver;
	}
}
///grant req to head of queue when withdraw

//=====================================================================================================================
class ReceiveThread1 implements Runnable{
	static Socket socket;
	static String msgToServer=null;
	static int replyFrom = 0;

	public ReceiveThread1(Socket s, int i) {
		System.out.println("Starting Receiver Thread for process: "+i);
		this.socket=s;
		this.replyFrom = i;
		//System.out.println("after constructor");
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
						System.out.println("-------------------"+msgToServer+"--------------------");
						String[] msg = msgToServer.split(" ");
						if(msg[0].equals("DATA")){
							System.out.println("-------------------"+msgToServer+"--------------------");
						}
						else{
							DataServer.startAlgo(msgToServer);
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